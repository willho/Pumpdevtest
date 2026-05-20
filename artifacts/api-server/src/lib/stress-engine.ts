import WebSocket from "ws";
import { db } from "@workspace/db";
import { tokensTable, tradesTable, resetsTable, rotationsTable, migrationsTable } from "@workspace/db/schema";
import { sql, eq, and, isNull, inArray, or } from "drizzle-orm";
import {
  state,
  log,
  sendToProxy,
  PER_PROVIDER_LIMIT,
  DETECTION_WINDOW,
  RESET_COOLDOWN,
  PUMPPORTAL_STALL_THRESHOLD,
} from "./stress-state.js";
import { startDiscoveryStreams } from "./coordinator.js";

// ---------------------------------------------------------------------------
// Trade resume latency
// ---------------------------------------------------------------------------

export async function checkTradeResumeCompletion(
  provider: string,
  tradeTime: number
) {
  try {
    const result = await db
      .select()
      .from(resetsTable)
      .where(
        and(
          eq(resetsTable.provider, provider),
          isNull(resetsTable.firstTradeAfterAt),
          sql`${resetsTable.resetTriggeredAt} <= ${tradeTime}`
        )
      )
      .orderBy(sql`${resetsTable.resetTriggeredAt} DESC`)
      .limit(1);

    if (result.length > 0) {
      const reset = result[0];
      const resumeLatency = tradeTime - reset.resetTriggeredAt;

      await db
        .update(resetsTable)
        .set({ firstTradeAfterAt: tradeTime, tradeResumeLatencyMs: resumeLatency })
        .where(eq(resetsTable.id, reset.id));

      state.tradeResumeStats.all.push(resumeLatency);
      state.tradeResumeStats.best = Math.min(state.tradeResumeStats.best, resumeLatency);
      state.tradeResumeStats.worst = Math.max(state.tradeResumeStats.worst, resumeLatency);
    }
  } catch (e: unknown) {
    log(`Resume check error: ${(e as Error).message}`, "error");
  }
}

// ---------------------------------------------------------------------------
// Mint assignment + subscription (shared by new-token and migration paths)
// ---------------------------------------------------------------------------

export async function assignAndSubscribeMint(mint: string) {
  if (state.seenMints.has(mint)) return;
  state.seenMints.add(mint);

  state.totalTokens++;
  const tokenIndex = state.totalTokens;

  // Fewest-subscriptions-first: always assign to the two least-loaded providers.
  // When a proxy rejoins with 0 subs it sits at the bottom and catches up automatically.
  const providersByLoad = [
    { id: "test", count: state.testSubscriptions.size },
    ...Array.from(state.proxies.entries()).map(([id, p]) => ({ id, count: p.subscriptions.size })),
  ].sort((a, b) => a.count - b.count);

  const provider1 = providersByLoad[0].id;
  const provider2 = providersByLoad.length > 1 ? providersByLoad[1].id : null;

  await db.insert(tokensTable).values({
    mint,
    provider1,
    provider2: provider2 ?? null,
    assignedAt: Date.now(),
  });

  // Attempt to subscribe to 2 providers in fewest-first order.
  // If a provider's WS is dead at assignment time, skip it and fall through
  // to the next least-loaded provider — the mint always gets 2 live subscribers.
  const assigned: string[] = [];
  for (const { id } of providersByLoad) {
    if (assigned.length >= 2) break;
    const ok = await handleAtCapacity(mint, id);
    if (ok) {
      assigned.push(id);
    } else {
      log(`[assign] Provider ${id.slice(0, 8)} unreachable, trying next`, "warn");
    }
  }

  const actualP1 = assigned[0] ?? provider1;
  const actualP2 = assigned[1] ?? null;

  if (actualP1 !== provider1 || actualP2 !== provider2) {
    await db
      .update(tokensTable)
      .set({ provider1: actualP1, provider2: actualP2 })
      .where(eq(tokensTable.mint, mint));
    log(
      `[assign] Fallback: ${mint.slice(0, 8)} → ${actualP1.slice(0, 8)}${actualP2 ? " + " + actualP2.slice(0, 8) : " (single-covered)"}`,
      "warn"
    );
  }

  log(
    `Token ${tokenIndex}: ${mint.slice(0, 8)}... → ${actualP1.slice(0, 8)}${actualP2 ? ", " + actualP2.slice(0, 8) : ""}`
  );

}

// ---------------------------------------------------------------------------
// PumpPortal (New Token Discovery — primary)
// ---------------------------------------------------------------------------

const PP_BACKOFF_MIN = 2_000;
const PP_BACKOFF_MAX = 60_000;
let ppBackoffMs = PP_BACKOFF_MIN;
let ppReconnectTimer: NodeJS.Timeout | undefined;

function schedulePumpPortalReconnect(penaltyMs?: number) {
  if (ppReconnectTimer) return;
  const delay = penaltyMs ?? ppBackoffMs;
  log(`[pumpportal] Reconnecting in ${Math.round(delay / 1000)}s`, "warn");
  ppReconnectTimer = setTimeout(() => {
    ppReconnectTimer = undefined;
    connectPumpPortal();
  }, delay);
  if (!penaltyMs) ppBackoffMs = Math.min(ppBackoffMs * 2, PP_BACKOFF_MAX);
}

function connectPumpPortal() {
  let ws: WebSocket;
  try {
    // Close any existing portal connection before opening a new one
    if (state.pumpPortalWs) {
      state.pumpPortalWs.removeAllListeners();
      state.pumpPortalWs.close();
      state.pumpPortalWs = null;
    }
    ws = new WebSocket("wss://pumpportal.fun/api/data");
    state.pumpPortalWs = ws;

    ws.on("open", () => {
      ppBackoffMs = PP_BACKOFF_MIN; // reset on success
      log("[pumpportal] Connected — subscribing to new tokens and migrations");
      ws.send(JSON.stringify({ method: "subscribeNewToken" }));
      ws.send(JSON.stringify({ method: "subscribeMigration" }));
      state.pumpPortalPingInterval = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) ws.ping();
      }, 30 * 60 * 1000);
    });

    ws.on("message", async (raw: Buffer) => {
      if (!state.isRunning) return;
      try {
        const data = JSON.parse(raw.toString());
        if (!data.mint) return;

        if (data.txType === "migrate") {
          state.totalMigrations++;
          const pool = String(data.pool ?? "unknown");
          log(`[migration] Graduated (${pool}): ${data.mint.slice(0, 8)}...`);
          await db.insert(migrationsTable).values({
            mint: data.mint,
            poolAddress: pool,
            signature: String(data.signature ?? ""),
            provider: "pumpportal",
            detectedAt: Date.now(),
            mintAmount: "0",
            solAmount: "0",
          }).onConflictDoNothing();
          return;
        }

        state.coordinatorLastNewTokenAt = Date.now();
        if (state.sourceNewToken) {
          await assignAndSubscribeMint(data.mint);
        } else {
          state.seenMints.add(data.mint);
        }
      } catch (e: unknown) {
        log(`[pumpportal] Parse error: ${(e as Error).message}`, "error");
      }
    });

    ws.on("close", (code, reason) => {
      log(`[pumpportal] Closed (${code}): ${reason || "no reason"}`, "warn");
      state.pumpPortalWs = null;
      if (state.pumpPortalPingInterval) {
        clearInterval(state.pumpPortalPingInterval);
        state.pumpPortalPingInterval = undefined;
      }
      if (!state.isRunning) return;
      // 1006 = abnormal TCP drop (rate-limit / ban) — always apply penalty
      const penalty = code === 1006 ? 30_000 : undefined;
      schedulePumpPortalReconnect(penalty);
    });

    ws.on("error", (err) => {
      const is403 = err.message.includes("403");
      log(`[pumpportal] Error: ${err.message}`, "error");
      if (is403 && state.isRunning) schedulePumpPortalReconnect(30_000);
    });
  } catch (e: unknown) {
    log(`[pumpportal] Failed to connect: ${(e as Error).message}`, "error");
    if (state.isRunning) schedulePumpPortalReconnect();
  }
}

// ---------------------------------------------------------------------------
// PumpDev — single connection for both trade stream and newToken backup
// ---------------------------------------------------------------------------

function err403(msg: string) { return msg.includes("403"); }

const PD_BACKOFF_MIN = 2_000;
const PD_BACKOFF_MAX = 60_000;
let pdBackoffMs = PD_BACKOFF_MIN;
let pdReconnectTimer: NodeJS.Timeout | undefined;

function schedulePumpDevReconnect(penaltyMs?: number) {
  if (pdReconnectTimer) return;
  const delay = penaltyMs ?? pdBackoffMs;
  log(`[pumpdev] Reconnecting in ${Math.round(delay / 1000)}s`, "warn");
  pdReconnectTimer = setTimeout(() => {
    pdReconnectTimer = undefined;
    connectPumpDev();
  }, delay);
  if (!penaltyMs) pdBackoffMs = Math.min(pdBackoffMs * 2, PD_BACKOFF_MAX);
}

export function connectPumpDev() {
  let ws: WebSocket;
  try {
    // Close stale connection before creating a new one
    if (state.testConnection) {
      state.testConnection.removeAllListeners();
      state.testConnection.close();
      state.testConnection = null;
    }
    // Reset stall clock immediately so the detector doesn't fire while the socket is opening
    state.pumpDevNewTokenLastAt = Date.now();
    ws = new WebSocket("wss://pumpdev.io/ws");
    state.testConnection = ws;
    state.testLastTradeAt = Date.now();

    ws.on("open", () => {
      pdBackoffMs = PD_BACKOFF_MIN;
      state.pumpDevNewTokenLastAt = Date.now();
      log("[pumpdev] Connected — subscribing to newToken stream and existing trade subscriptions");
      ws.send(JSON.stringify({ method: "subscribeNewToken" }));
      if (state.testSubscriptions.size > 0) {
        const mints = [...state.testSubscriptions];
        for (let i = 0; i < mints.length; i += 100) {
          ws.send(JSON.stringify({ method: "subscribeTokenTrade", keys: mints.slice(i, i + 100) }));
        }
        log(`[pumpdev] Re-subscribed to ${mints.length} existing trade mints`);
      }
    });

    ws.on("message", async (raw: Buffer) => {
      if (!state.isRunning) return;
      try {
        const data = JSON.parse(raw.toString());
        if (!data.mint) return;

        if (data.signature) {
          // Trade event
          state.totalTrades++;
          const now = Date.now();
          state.testLastTradeAt = now;
          state.testIsStalled = false;
          const wallet: string | undefined = data.traderPublicKey ?? data.buyer ?? undefined;
          if (wallet) state.uniqueWallets.add(wallet);
          await db.insert(tradesTable).values({
            mint: data.mint,
            provider: "test",
            signature: data.signature,
            wallet: wallet ?? null,
            receivedAt: now,
          });
          await checkTradeResumeCompletion("test", now);
        } else if (data.txType !== "migrate") {
          // New token event
          state.pumpDevNewTokenLastAt = Date.now();
          state.pumpDevNewTokenIsStalled = false;
          if (state.sourceNewToken) {
            await assignAndSubscribeMint(data.mint);
          } else {
            state.seenMints.add(data.mint);
          }
        }
      } catch (e: unknown) {
        log(`[pumpdev] Error: ${(e as Error).message}`, "error");
      }
    });

    ws.on("close", (code, reason) => {
      log(`[pumpdev] Closed (${code}): ${reason || "no reason"}`, "warn");
      if (!state.isRunning) return;
      const penalty = err403(String(reason)) ? 30_000 : undefined;
      schedulePumpDevReconnect(penalty);
    });

    ws.on("error", (err) => {
      log(`[pumpdev] Error: ${err.message}`, "error");
      if (err403(err.message) && state.isRunning) schedulePumpDevReconnect(30_000);
    });
  } catch (e: unknown) {
    log(`[pumpdev] Failed to connect: ${(e as Error).message}`, "error");
    if (state.isRunning) schedulePumpDevReconnect();
  }
}

// ---------------------------------------------------------------------------
// Subscription routing
// ---------------------------------------------------------------------------

function providerSubscribe(target: string, mints: string[]): boolean {
  if (mints.length === 0) return false;

  if (target === "test") {
    const ws = state.testConnection;
    if (!ws || ws.readyState !== WebSocket.OPEN) return false;
    ws.send(JSON.stringify({ method: "subscribeTokenTrade", keys: mints }));
    for (const m of mints) state.testSubscriptions.add(m);
    return true;
  }

  const proxy = state.proxies.get(target);
  if (!proxy) return false;
  const sent = sendToProxy(target, { type: "subscribe", tokens: mints });
  if (sent) for (const m of mints) proxy.subscriptions.add(m);
  return sent;
}

function providerUnsubscribe(target: string, mint: string): boolean {
  if (target === "test") {
    const ws = state.testConnection;
    if (!ws || ws.readyState !== WebSocket.OPEN) return false;
    ws.send(JSON.stringify({ method: "unsubscribeTokenTrade", keys: [mint] }));
    state.testSubscriptions.delete(mint);
    return true;
  }

  const proxy = state.proxies.get(target);
  if (!proxy) return false;
  const sent = sendToProxy(target, { type: "unsubscribe", tokens: [mint] });
  if (sent) proxy.subscriptions.delete(mint);
  return sent;
}

function providerSubCount(target: string): number {
  if (target === "test") return state.testSubscriptions.size;
  return state.proxies.get(target)?.subscriptions.size ?? 0;
}

function refreshTotalSubCount() {
  state.subscriptionsCount =
    state.testSubscriptions.size +
    Array.from(state.proxies.values()).reduce(
      (sum, p) => sum + p.subscriptions.size,
      0
    );
}

// ---------------------------------------------------------------------------
// Capacity & rotation
// ---------------------------------------------------------------------------

async function handleAtCapacity(mint: string, target: string): Promise<boolean> {
  // Early exit if provider is not reachable — skip rotation work and DB
  // updates for dead providers so the caller can fall through to a live one.
  if (target === "test") {
    if (!state.testConnection || state.testConnection.readyState !== WebSocket.OPEN) return false;
  } else {
    if (!state.proxies.has(target)) return false;
  }

  if (providerSubCount(target) >= PER_PROVIDER_LIMIT) {
    const subscribedMints = Array.from(
      target === "test"
        ? state.testSubscriptions
        : (state.proxies.get(target)?.subscriptions ?? new Set<string>())
    );

    let tokenToEvict: string | null = null;

    if (subscribedMints.length > 0) {
      // Find the token assigned to this provider with least recent trade activity.
      // Filter by both provider assignment in DB and in-memory subscription set
      // so the candidate pool stays accurate even under partial state drift.
      const lastTrades = await db
        .select({
          mint: tradesTable.mint,
          lastTradeAt: sql<number>`MAX(${tradesTable.receivedAt})`.as("last_trade_at"),
        })
        .from(tradesTable)
        .where(inArray(tradesTable.mint, subscribedMints))
        .groupBy(tradesTable.mint);

      const lastTradeMap = new Map(lastTrades.map((r) => [r.mint, r.lastTradeAt]));

      const tokenRows = await db
        .select({ mint: tokensTable.mint, assignedAt: tokensTable.assignedAt })
        .from(tokensTable)
        .where(
          and(
            inArray(tokensTable.mint, subscribedMints),
            or(eq(tokensTable.provider1, target), eq(tokensTable.provider2, target))
          )
        );

      let quietestActivity = Infinity;
      for (const row of tokenRows) {
        const activity = lastTradeMap.get(row.mint) ?? row.assignedAt;
        if (activity < quietestActivity) {
          quietestActivity = activity;
          tokenToEvict = row.mint;
        }
      }
    }

    // Fall back to FIFO if activity query returned nothing
    if (!tokenToEvict) {
      tokenToEvict =
        target === "test"
          ? (state.testSubscriptions.values().next().value ?? null)
          : (state.proxies.get(target)?.subscriptions.values().next().value ?? null);
    }

    if (tokenToEvict) {
      const now = Date.now();

      // Collect final metrics and both provider assignments before eviction
      const [lastTradeRows, tradeData, tokenInfo] = await Promise.all([
        db
          .select({ receivedAt: tradesTable.receivedAt })
          .from(tradesTable)
          .where(eq(tradesTable.mint, tokenToEvict))
          .orderBy(sql`${tradesTable.receivedAt} DESC`)
          .limit(1),
        db
          .select({ wallet: tradesTable.wallet })
          .from(tradesTable)
          .where(eq(tradesTable.mint, tokenToEvict)),
        db
          .select({
            assignedAt: tokensTable.assignedAt,
            provider1: tokensTable.provider1,
            provider2: tokensTable.provider2,
          })
          .from(tokensTable)
          .where(eq(tokensTable.mint, tokenToEvict))
          .limit(1),
      ]);

      const assignedAt = tokenInfo[0]?.assignedAt ?? now;
      const lastTradeAt = lastTradeRows.length > 0 ? lastTradeRows[0].receivedAt : assignedAt;
      const timeSinceLastTradeMs = now - lastTradeAt;
      const uniqueBuyers = new Set(tradeData.map((t) => t.wallet).filter(Boolean)).size;
      const volume = tradeData.length;
      const graduatedAt = null;

      // Record rotation metrics
      await db.insert(rotationsTable).values({
        mint: tokenToEvict,
        discoveredAt: assignedAt,
        graduatedAt: graduatedAt ?? null,
        lastTradeAt,
        rotatedAt: now,
        timeSinceLastTradeMs,
        ageMs: now - assignedAt,
        uniqueBuyers: uniqueBuyers.toString(),
        totalVolumeSol: volume.toString(),
      });

      // Unsubscribe from ALL providers that hold this token and free their
      // in-memory slots. Both providers must be cleaned up before deleting the
      // DB row to prevent ghost subscriptions consuming capacity indefinitely.
      const allProviders = [
        tokenInfo[0]?.provider1 ?? null,
        tokenInfo[0]?.provider2 ?? null,
      ];
      for (const provider of allProviders) {
        if (!provider) continue;
        const subSet =
          provider === "test"
            ? state.testSubscriptions
            : state.proxies.get(provider)?.subscriptions;
        if (!subSet?.has(tokenToEvict)) continue;
        // Send WS notification (best-effort), then force-free the in-memory slot
        // so the capacity count is correct even if the send fails.
        providerUnsubscribe(provider, tokenToEvict);
        subSet.delete(tokenToEvict);
      }

      await db.delete(tokensTable).where(eq(tokensTable.mint, tokenToEvict));
      state.rotationCount++;
      log(
        `Rotation [${target.slice(0, 8)}]: dropped ${tokenToEvict.slice(0, 8)}... (${Math.round(timeSinceLastTradeMs / 1000)}s inactive), adding ${mint.slice(0, 8)}...`
      );
    }
  }

  const ok = providerSubscribe(target, [mint]);
  if (ok) {
    await db
      .update(tokensTable)
      .set({ subscribedAt: Date.now() })
      .where(eq(tokensTable.mint, mint));
  }

  refreshTotalSubCount();
  return ok;
}

// ---------------------------------------------------------------------------
// Stall detection
// ---------------------------------------------------------------------------

export async function detectStalls() {
  if (!state.isRunning) return;
  if (state.totalTokens < 10) return;

  const now = Date.now();
  let stalledCount = 0;

  // Test provider (trade stream)
  if (state.testSubscriptions.size >= 10) {
    if (now - state.testLastTradeAt > DETECTION_WINDOW) {
      if (now - state.testLastResetAt > RESET_COOLDOWN) {
        state.testIsStalled = true;
        state.testLastResetAt = now;
        const silentMs = Math.round((now - state.testLastTradeAt) / 1000);
        await db.insert(resetsTable).values({ provider: "test", resetTriggeredAt: now });
        log(`[test] STALL — ${silentMs}s silent — resetting connection`, "warn");
        state.testConnection?.close();
      }
      stalledCount++;
    } else {
      state.testIsStalled = false;
    }
  }

  // Proxy providers (trade streams)
  for (const [proxyId, proxy] of state.proxies) {
    if (proxy.subscriptions.size < 10) continue;
    if (now - proxy.lastTradeAt > DETECTION_WINDOW) {
      if (now - proxy.lastResetAt > RESET_COOLDOWN) {
        proxy.isStalled = true;
        proxy.lastResetAt = now;
        const silentMs = Math.round((now - proxy.lastTradeAt) / 1000);
        await db.insert(resetsTable).values({ provider: proxyId, resetTriggeredAt: now });
        log(`[${proxy.name}] STALL — ${silentMs}s silent — sending reset`, "warn");
        sendToProxy(proxyId, { type: "reset" });
      }
      stalledCount++;
    } else {
      proxy.isStalled = false;
    }
  }

  // Simultaneous stall detection (trade streams)
  const wasSimultaneous = state.simultaneousStall;
  state.simultaneousStall = stalledCount >= 2;
  if (state.simultaneousStall && !wasSimultaneous) {
    log(`!! SIMULTANEOUS STALL — ${stalledCount} providers stalled at once`, "error");
  }

  // PumpPortal stream stall detection
  const coordinatorHasToken = (now - state.coordinatorLastNewTokenAt) <= PUMPPORTAL_STALL_THRESHOLD;
  const activeProxyStreams = Array.from(state.proxies.values()).filter(
    (p) => (now - p.pumpPortalLastNewTokenAt) <= PUMPPORTAL_STALL_THRESHOLD
  ).length;
  const activeStreams = (coordinatorHasToken ? 1 : 0) + activeProxyStreams;

  // PumpPortal: reconnect only when stalled and PumpDev backup is healthy
  if (!coordinatorHasToken && activeProxyStreams > 0) {
    log(
      `[pumpportal] Coordinator stalled (silent ${Math.round((now - state.coordinatorLastNewTokenAt) / 1000)}s), reconnecting`,
      "warn"
    );
    if (state.pumpPortalPingInterval) {
      clearInterval(state.pumpPortalPingInterval);
      state.pumpPortalPingInterval = undefined;
    }
    schedulePumpPortalReconnect();
  }

  for (const [proxyId, proxy] of state.proxies) {
    if ((now - proxy.pumpPortalLastNewTokenAt) > PUMPPORTAL_STALL_THRESHOLD && activeProxyStreams < state.proxies.size) {
      log(
        `[pumpportal] ${proxy.name} stalled (silent ${Math.round((now - proxy.pumpPortalLastNewTokenAt) / 1000)}s), sending reset`,
        "warn"
      );
      sendToProxy(proxyId, { type: "reset_pumpportal" });
    } else {
      proxy.pumpPortalIsStalled = false;
    }
  }

  // PumpDev newToken backup stall detection (same connection as trade stream)
  const pumpDevNewTokenHealthy = (now - state.pumpDevNewTokenLastAt) <= PUMPPORTAL_STALL_THRESHOLD;
  if (!pumpDevNewTokenHealthy) {
    state.pumpDevNewTokenIsStalled = true;
    if (coordinatorHasToken) {
      // PumpPortal is healthy — reconnect PumpDev (trade+newToken) individually
      log(`[pumpdev] Stalled (silent ${Math.round((now - state.pumpDevNewTokenLastAt) / 1000)}s), reconnecting`, "warn");
      schedulePumpDevReconnect();
    }
  } else {
    state.pumpDevNewTokenIsStalled = false;
  }

  // All discovery streams (coordinator PumpPortal + PumpDev backup + proxy PumpPortals)
  const nowSimultaneouslyStalled = activeStreams === 0 && !pumpDevNewTokenHealthy;
  if (nowSimultaneouslyStalled && !state.wasPumpPortalSimultaneouslyStalled) {
    log(`[CRITICAL] ALL discovery streams stalled — no token coverage`, "error");
    state.wasPumpPortalSimultaneouslyStalled = true;
    state.seenMints.clear();
    if (state.pumpPortalPingInterval) {
      clearInterval(state.pumpPortalPingInterval);
      state.pumpPortalPingInterval = undefined;
    }
    schedulePumpPortalReconnect();
    schedulePumpDevReconnect();
    for (const proxyId of state.proxies.keys()) {
      sendToProxy(proxyId, { type: "reset_pumpportal" });
    }
  } else if (!nowSimultaneouslyStalled && state.wasPumpPortalSimultaneouslyStalled) {
    log(`[discovery] Coverage restored`, "warn");
    state.wasPumpPortalSimultaneouslyStalled = false;
  }
  state.simultaneousPumpPortalStall = nowSimultaneouslyStalled;

  // 24-hour time limit
  if (state.testStartAt > 0 && now - state.testStartAt >= 24 * 60 * 60 * 1000) {
    log("24-hour time limit reached — stopping test", "warn");
    stopTest();
  }
}

// ---------------------------------------------------------------------------
// Start / stop
// ---------------------------------------------------------------------------

export async function startTest(sourceNewToken: boolean) {
  if (state.isRunning) return;

  state.isRunning = true;
  state.mode = "RUNNING";
  state.sourceNewToken = sourceNewToken;
  state.logs = [];
  state.subscriptionsCount = 0;
  state.rotationCount = 0;
  state.reconnectStats = { best: Infinity, worst: 0, all: [] };
  state.tradeResumeStats = { best: Infinity, worst: 0, all: [] };
  state.totalTokens = 0;
  state.totalTrades = 0;
  state.totalRotations = 0;
  state.testSubscriptions = new Set();
  state.testLastTradeAt = Date.now();
  state.testIsStalled = false;
  state.testLastResetAt = 0;
  state.coordinatorLastNewTokenAt = Date.now();
  state.pumpDevNewTokenLastAt = Date.now();
  state.pumpDevNewTokenIsStalled = false;
  state.seenMints = new Set();
  state.simultaneousPumpPortalStall = false;
  state.wasPumpPortalSimultaneouslyStalled = false;
  state.uniqueWallets = new Set();
  state.testStartAt = Date.now();

  // Clean up any connections left over from a previous run before starting fresh
  if (pdReconnectTimer) { clearTimeout(pdReconnectTimer); pdReconnectTimer = undefined; }
  if (ppReconnectTimer) { clearTimeout(ppReconnectTimer); ppReconnectTimer = undefined; }

  for (const proxy of state.proxies.values()) {
    proxy.subscriptions = new Set();
    proxy.isStalled = false;
    proxy.lastResetAt = 0;
    proxy.lastTradeAt = Date.now();
    proxy.pumpPortalLastNewTokenAt = Date.now();
    proxy.pumpPortalIsStalled = false;
  }

  const limit = (1 + state.proxies.size) * PER_PROVIDER_LIMIT;
  log(`TEST STARTED (Sources: ${sourceNewToken ? "NEW_TOKEN" : "NONE"}, Limit: ${limit}, Proxies: ${state.proxies.size})`);

  await db.execute(sql`TRUNCATE tokens, trades, resets, migrations, rotations`);

  connectPumpDev();
  connectPumpPortal();

  await new Promise((resolve) => setTimeout(resolve, 1000));

  startDiscoveryStreams();

  setInterval(() => detectStalls(), 1000);
}

export async function autoResumeTest() {
  if (state.isRunning) return;

  const existingTokens = await db
    .select({
      mint: tokensTable.mint,
      provider1: tokensTable.provider1,
      provider2: tokensTable.provider2,
    })
    .from(tokensTable);

  if (existingTokens.length === 0) {
    log("[resume] No existing tokens — starting fresh");
    await startTest(true);
    return;
  }

  // Clean up any connections left over from a previous crashed run before starting fresh
  if (pdReconnectTimer) { clearTimeout(pdReconnectTimer); pdReconnectTimer = undefined; }
  if (ppReconnectTimer) { clearTimeout(ppReconnectTimer); ppReconnectTimer = undefined; }

  state.isRunning = true;
  state.mode = "RUNNING";
  state.sourceNewToken = true;
  state.logs = [];
  state.subscriptionsCount = 0;
  state.rotationCount = 0;
  state.reconnectStats = { best: Infinity, worst: 0, all: [] };
  state.tradeResumeStats = { best: Infinity, worst: 0, all: [] };
  state.totalTokens = existingTokens.length;
  state.totalTrades = 0;
  state.totalRotations = 0;
  state.testSubscriptions = new Set();
  state.testLastTradeAt = Date.now();
  state.testIsStalled = false;
  state.testLastResetAt = 0;
  state.coordinatorLastNewTokenAt = Date.now();
  state.pumpDevNewTokenLastAt = Date.now();
  state.pumpDevNewTokenIsStalled = false;
  state.seenMints = new Set();
  state.simultaneousPumpPortalStall = false;
  state.wasPumpPortalSimultaneouslyStalled = false;
  state.uniqueWallets = new Set();
  state.testStartAt = Date.now();

  for (const token of existingTokens) {
    state.seenMints.add(token.mint);
    if (token.provider1 === "test" || token.provider2 === "test") {
      state.testSubscriptions.add(token.mint);
    }
  }

  log(`[resume] Resuming: ${existingTokens.length} tokens, ${state.testSubscriptions.size} on test`);

  connectPumpDev();
  connectPumpPortal();

  await new Promise((resolve) => setTimeout(resolve, 1000));

  startDiscoveryStreams();

  setInterval(() => detectStalls(), 1000);
}

export function stopTest() {
  state.isRunning = false;
  if (state.testConnection) state.testConnection.close();
  if (state.pumpPortalPingInterval) {
    clearInterval(state.pumpPortalPingInterval);
    state.pumpPortalPingInterval = undefined;
  }
  if (pdReconnectTimer) {
    clearTimeout(pdReconnectTimer);
    pdReconnectTimer = undefined;
  }
  if (ppReconnectTimer) {
    clearTimeout(ppReconnectTimer);
    ppReconnectTimer = undefined;
  }
  for (const proxyId of state.proxies.keys()) {
    sendToProxy(proxyId, { type: "reset" });
  }
  log("TEST STOPPED", "warn");
}
