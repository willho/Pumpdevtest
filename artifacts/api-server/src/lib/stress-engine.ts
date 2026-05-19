import WebSocket from "ws";
import { db } from "@workspace/db";
import { tokensTable, tradesTable, resetsTable, migrationsTable, rotationsTable } from "@workspace/db/schema";
import { sql, eq, and, isNull } from "drizzle-orm";
import {
  state,
  log,
  sendToProxy,
  CAPACITY_LIMITS,
  PER_PROVIDER_LIMIT,
  DETECTION_WINDOW,
  RESET_COOLDOWN,
  PUMPPORTAL_STALL_THRESHOLD,
  MIGRATION_STALL_THRESHOLD,
} from "./stress-state.js";
import { startMigrationDetection, stopMigrationDetection } from "./migration-engine.js";
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
// PumpPortal (New Token Discovery)
// ---------------------------------------------------------------------------

function connectPumpPortal() {
  const wsUrl = "wss://pumpportal.fun/api/data";
  let ws: WebSocket;
  let tokenIndex = state.totalTokens;

  try {
    ws = new WebSocket(wsUrl);

    ws.on("open", () => {
      log("[pumpportal] Connected to PumpPortal");
      ws.send(JSON.stringify({ method: "subscribeNewToken" }));

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
        state.coordinatorLastNewTokenAt = now;

        if (state.seenMints.has(data.mint)) {
          return;
        }

        state.seenMints.add(data.mint);
        tokenIndex++;
        state.totalTokens = tokenIndex;

        const mode = state.mode;
        if (!mode) return;

        const limit = CAPACITY_LIMITS[mode];
        const allProviders = ["test", ...Array.from(state.proxies.keys())];

        let provider1: string;
        let provider2: string | null = null;

        if (mode === "SINGLE") {
          provider1 = "test";
        } else if (mode === "DUAL") {
          provider1 = "test";
          provider2 = allProviders[1] ?? null;
        } else {
          const pool = allProviders.slice(0, 3);
          provider1 = pool[tokenIndex % pool.length];
          provider2 = pool[(tokenIndex + 1) % pool.length];
        }

        await db.insert(tokensTable).values({
          mint: data.mint,
          provider1,
          provider2,
          assignedAt: now,
        });

        await handleAtCapacity(data.mint, provider1);
        if (provider2 && provider2 !== provider1) {
          await handleAtCapacity(data.mint, provider2);
        }

        log(
          `Token ${tokenIndex}: ${data.mint.slice(0, 8)}... → ${provider1.slice(0, 8)}${provider2 ? ", " + provider2.slice(0, 8) : ""}`
        );

        if (state.totalTokens >= limit) {
          log(
            `Reached capacity limit (${state.totalTokens}/${limit} unique tokens)`
          );
          state.isRunning = false;
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

async function handleAtCapacity(mint: string, target: string) {
  if (providerSubCount(target) >= PER_PROVIDER_LIMIT) {
    const oldest =
      target === "test"
        ? state.testSubscriptions.values().next().value
        : state.proxies.get(target)?.subscriptions.values().next().value;

    if (oldest) {
      providerUnsubscribe(target, oldest);
      await db
        .update(tokensTable)
        .set({ unsubscribedAt: Date.now() })
        .where(eq(tokensTable.mint, oldest));
      state.rotationCount++;
      log(
        `Rotation [${target.slice(0, 8)}]: dropped ${oldest.slice(0, 8)}..., adding ${mint.slice(0, 8)}...`
      );
    }
  }

  providerSubscribe(target, [mint]);
  await db
    .update(tokensTable)
    .set({ subscribedAt: Date.now() })
    .where(eq(tokensTable.mint, mint));

  refreshTotalSubCount();
}

// ---------------------------------------------------------------------------
// Stall detection
// ---------------------------------------------------------------------------

export async function detectStalls() {
  if (!state.isRunning || !state.mode) return;

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

  // Per-stream: coordinator PumpPortal stall
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

  // Per-stream: proxy PumpPortal stalls
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

  // Simultaneous PumpPortal stall (CRITICAL — all streams dead)
  const nowSimultaneouslyStalled = activeStreams === 0;
  if (nowSimultaneouslyStalled && !state.wasPumpPortalSimultaneouslyStalled) {
    log(
      `[CRITICAL] ALL PumpPortal streams stalled — NO TOKEN DISCOVERY COVERAGE`,
      "error"
    );
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
    log(
      `[pumpportal] Coverage restored (simultaneous stall cleared)`,
      "warn"
    );
    state.wasPumpPortalSimultaneouslyStalled = false;
  }
  state.simultaneousPumpPortalStall = nowSimultaneouslyStalled;

  // Migration stream stall detection (TRIPLE mode only)
  if (state.mode === "TRIPLE") {
    checkMigrationProviderStall();
  }

  // 24-hour time limit
  if (state.testStartAt > 0 && now - state.testStartAt >= 24 * 60 * 60 * 1000) {
    log("24-hour time limit reached — stopping test", "warn");
    stopTest();
  }
}

// ---------------------------------------------------------------------------
// Start / stop
// ---------------------------------------------------------------------------

export async function startTest(mode: "SINGLE" | "DUAL" | "TRIPLE") {
  if (state.isRunning) return;

  state.isRunning = true;
  state.mode = mode;
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
  state.totalMigrations = 0;
  state.totalRotations = 0;
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

  const limit = CAPACITY_LIMITS[mode];
  log(`TEST STARTED (Mode: ${mode}, Limit: ${limit} tokens, Proxies: ${state.proxies.size})`);

  await db.execute(sql`TRUNCATE tokens, trades, resets, migrations, rotations`);

  connectTestPumpDev();
  connectPumpPortal();

  if (mode === "TRIPLE") {
    await startMigrationDetection();
    log("[migration] TRIPLE mode active — migration detection started");
  }

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
  stopMigrationDetection().catch(() => {});
  log("TEST STOPPED", "warn");
}
