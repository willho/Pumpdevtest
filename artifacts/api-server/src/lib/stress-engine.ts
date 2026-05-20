import WebSocket from "ws";
import { db } from "@workspace/db";
import { tokensTable, tradesTable, resetsTable, migrationsTable, rotationsTable } from "@workspace/db/schema";
import { sql, eq, and, isNull, inArray, or } from "drizzle-orm";
import {
  state,
  log,
  sendToProxy,
  PER_PROVIDER_LIMIT,
  DETECTION_WINDOW,
  RESET_COOLDOWN,
  PUMPPORTAL_STALL_THRESHOLD,
  MIGRATION_STALL_THRESHOLD,
} from "./stress-state.js";
import { checkMigrationProviderStall, startDiscoveryStreams } from "./coordinator.js";

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
// PumpPortal (New Token Discovery + Migration Detection)
// ---------------------------------------------------------------------------

function connectPumpPortal() {
  const wsUrl = "wss://pumpportal.fun/api/data";
  let ws: WebSocket;

  try {
    ws = new WebSocket(wsUrl);

    ws.on("open", () => {
      log("[pumpportal] Connected — subscribing to new tokens and migrations");
      ws.send(JSON.stringify({ method: "subscribeNewToken" }));
      ws.send(JSON.stringify({ method: "subscribeMigration" }));

      state.pumpPortalPingInterval = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.ping();
        }
      }, 30 * 60 * 1000);
    });

    ws.on("message", async (raw: Buffer) => {
      if (!state.isRunning) return;

      try {
        const data = JSON.parse(raw.toString());
        if (!data.mint) return;

        const now = Date.now();

        // Migration event
        if (data.txType === "migrate") {
          const pool = String(data.pool ?? "unknown");
          const isPumpSwap = pool === "pump-amm";
          state.migrationProviderLastEventAt.set("pumpportal", now);
          state.migrationProvidersStalled.delete("pumpportal");
          state.totalMigrations++;

          if (isPumpSwap) {
            log(`[migration] Graduated (PumpSwap): ${data.mint.slice(0, 8)}... sig: ${String(data.signature).slice(0, 8)}...`);
          } else {
            state.nonPumpSwapMigrations++;
            log(`[migration] Graduated (non-PumpSwap): ${data.mint.slice(0, 8)}... pool: ${pool}`);
          }

          await db.insert(migrationsTable).values({
            mint: data.mint,
            poolAddress: pool,
            signature: data.signature ?? "",
            provider: "pumpportal",
            detectedAt: now,
            mintAmount: "0",
            solAmount: "0",
          }).onConflictDoNothing();

          if (isPumpSwap && state.sourceMigration) {
            await assignAndSubscribeMint(data.mint);
          }
          return;
        }

        // New token event
        state.coordinatorLastNewTokenAt = now;

        if (state.sourceNewToken) {
          await assignAndSubscribeMint(data.mint);
        } else {
          state.seenMints.add(data.mint);
        }
      } catch (e: unknown) {
        log(`[pumpportal] Error: ${(e as Error).message}`, "error");
      }
    });

    ws.on("close", (code, reason) => {
      log(
        `[pumpportal] Closed (code ${code}): ${reason || "no reason"}`,
        "warn"
      );
      if (state.pumpPortalPingInterval) {
        clearInterval(state.pumpPortalPingInterval);
        state.pumpPortalPingInterval = undefined;
      }
      if (state.isRunning) {
        setTimeout(() => connectPumpPortal(), 2000);
      }
    });

    ws.on("error", (err) => {
      log(`[pumpportal] Error: ${err.message}`, "error");
    });
  } catch (e: unknown) {
    log(`[pumpportal] Failed to connect: ${(e as Error).message}`, "error");
  }
}

// ---------------------------------------------------------------------------
// Test provider (coordinator's own PumpDev connection)
// ---------------------------------------------------------------------------

export function connectTestPumpDev() {
  const wsUrl = "wss://pumpdev.io/ws";
  let ws: WebSocket;

  try {
    ws = new WebSocket(wsUrl);
    state.testConnection = ws;
    state.testLastTradeAt = Date.now();

    ws.on("open", () => {
      log("[test] Connected to PumpDev");
      if (state.testSubscriptions.size > 0) {
        const mints = [...state.testSubscriptions];
        for (let i = 0; i < mints.length; i += 100) {
          ws.send(JSON.stringify({ method: "subscribeTokenTrade", keys: mints.slice(i, i + 100) }));
        }
        log(`[test] Re-subscribed to ${mints.length} existing mints`);
      }
    });

    ws.on("message", async (raw: Buffer) => {
      try {
        const data = JSON.parse(raw.toString());
        if (data.error || !data.signature) return;

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
      } catch (e: unknown) {
        log(`[test] Error: ${(e as Error).message}`, "error");
      }
    });

    ws.on("error", (err) => {
      log(`[test] WebSocket error: ${err.message}`, "error");
    });

    ws.on("close", () => {
      log("[test] Connection closed — reconnecting in 2s", "warn");
      if (state.isRunning) setTimeout(() => connectTestPumpDev(), 2000);
    });
  } catch (e: unknown) {
    log(`[test] Failed to connect: ${(e as Error).message}`, "error");
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
      const [lastTradeRows, tradeData, tokenInfo, migrationRows] = await Promise.all([
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
        db
          .select({ detectedAt: migrationsTable.detectedAt })
          .from(migrationsTable)
          .where(eq(migrationsTable.mint, tokenToEvict))
          .limit(1),
      ]);

      const assignedAt = tokenInfo[0]?.assignedAt ?? now;
      const lastTradeAt = lastTradeRows.length > 0 ? lastTradeRows[0].receivedAt : assignedAt;
      const timeSinceLastTradeMs = now - lastTradeAt;
      const uniqueBuyers = new Set(tradeData.map((t) => t.wallet).filter(Boolean)).size;
      const volume = tradeData.length;
      const graduatedAt = migrationRows.length > 0 ? migrationRows[0].detectedAt : null;

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

  if (!coordinatorHasToken && activeProxyStreams > 0) {
    log(
      `[pumpportal] Coordinator stalled (silent ${Math.round((now - state.coordinatorLastNewTokenAt) / 1000)}s), reconnecting`,
      "warn"
    );
    if (state.pumpPortalPingInterval) {
      clearInterval(state.pumpPortalPingInterval);
      state.pumpPortalPingInterval = undefined;
    }
    setTimeout(() => connectPumpPortal(), 0);
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

  const nowSimultaneouslyStalled = activeStreams === 0;
  if (nowSimultaneouslyStalled && !state.wasPumpPortalSimultaneouslyStalled) {
    log(`[CRITICAL] ALL PumpPortal streams stalled — NO TOKEN DISCOVERY COVERAGE`, "error");
    state.wasPumpPortalSimultaneouslyStalled = true;
    state.seenMints.clear();
    if (state.pumpPortalPingInterval) {
      clearInterval(state.pumpPortalPingInterval);
      state.pumpPortalPingInterval = undefined;
    }
    setTimeout(() => connectPumpPortal(), 0);
    for (const proxyId of state.proxies.keys()) {
      sendToProxy(proxyId, { type: "reset_pumpportal" });
    }
  } else if (!nowSimultaneouslyStalled && state.wasPumpPortalSimultaneouslyStalled) {
    log(`[pumpportal] Coverage restored (simultaneous stall cleared)`, "warn");
    state.wasPumpPortalSimultaneouslyStalled = false;
  }
  state.simultaneousPumpPortalStall = nowSimultaneouslyStalled;

  // Migration stream stall detection
  checkMigrationProviderStall();

  // 24-hour time limit
  if (state.testStartAt > 0 && now - state.testStartAt >= 24 * 60 * 60 * 1000) {
    log("24-hour time limit reached — stopping test", "warn");
    stopTest();
  }
}

// ---------------------------------------------------------------------------
// Start / stop
// ---------------------------------------------------------------------------

export async function startTest(sourceNewToken: boolean, sourceMigration: boolean) {
  if (state.isRunning) return;

  state.isRunning = true;
  state.mode = "RUNNING";
  state.sourceNewToken = sourceNewToken;
  state.sourceMigration = sourceMigration;
  state.logs = [];
  state.subscriptionsCount = 0;
  state.rotationCount = 0;
  state.reconnectStats = { best: Infinity, worst: 0, all: [] };
  state.tradeResumeStats = { best: Infinity, worst: 0, all: [] };
  state.totalTokens = 0;
  state.totalTrades = 0;
  state.totalMigrations = 0;
  state.totalRotations = 0;
  state.testSubscriptions = new Set();
  state.testLastTradeAt = Date.now();
  state.testIsStalled = false;
  state.testLastResetAt = 0;
  state.coordinatorLastNewTokenAt = Date.now();
  state.seenMints = new Set();
  state.simultaneousPumpPortalStall = false;
  state.wasPumpPortalSimultaneouslyStalled = false;
  state.uniqueWallets = new Set();
  state.testStartAt = Date.now();
  state.migrationProviderLastEventAt = new Map();
  state.migrationProvidersStalled = new Set();

  for (const proxy of state.proxies.values()) {
    proxy.subscriptions = new Set();
    proxy.isStalled = false;
    proxy.lastResetAt = 0;
    proxy.lastTradeAt = Date.now();
    proxy.pumpPortalLastNewTokenAt = Date.now();
    proxy.pumpPortalIsStalled = false;
  }

  const limit = (1 + state.proxies.size) * PER_PROVIDER_LIMIT;
  const sources = [sourceNewToken ? "NEW_TOKEN" : null, sourceMigration ? "MIGRATION" : null]
    .filter(Boolean).join("+") || "NONE";
  log(`TEST STARTED (Sources: ${sources}, Limit: ${limit} tokens, Proxies: ${state.proxies.size})`);

  await db.execute(sql`TRUNCATE tokens, trades, resets, migrations, rotations`);

  connectTestPumpDev();
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
    await startTest(true, false);
    return;
  }

  state.isRunning = true;
  state.mode = "RUNNING";
  state.sourceNewToken = true;
  state.sourceMigration = false;
  state.logs = [];
  state.subscriptionsCount = 0;
  state.rotationCount = 0;
  state.reconnectStats = { best: Infinity, worst: 0, all: [] };
  state.tradeResumeStats = { best: Infinity, worst: 0, all: [] };
  state.totalTokens = existingTokens.length;
  state.totalTrades = 0;
  state.totalMigrations = 0;
  state.totalRotations = 0;
  state.testSubscriptions = new Set();
  state.testLastTradeAt = Date.now();
  state.testIsStalled = false;
  state.testLastResetAt = 0;
  state.coordinatorLastNewTokenAt = Date.now();
  state.seenMints = new Set();
  state.simultaneousPumpPortalStall = false;
  state.wasPumpPortalSimultaneouslyStalled = false;
  state.uniqueWallets = new Set();
  state.testStartAt = Date.now();
  state.migrationProviderLastEventAt = new Map();
  state.migrationProvidersStalled = new Set();

  for (const token of existingTokens) {
    state.seenMints.add(token.mint);
    if (token.provider1 === "test" || token.provider2 === "test") {
      state.testSubscriptions.add(token.mint);
    }
  }

  log(`[resume] Resuming: ${existingTokens.length} tokens, ${state.testSubscriptions.size} on test`);

  connectTestPumpDev();
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
  for (const proxyId of state.proxies.keys()) {
    sendToProxy(proxyId, { type: "reset" });
  }
  log("TEST STOPPED", "warn");
}
