import WebSocket from "ws";
import { db } from "@workspace/db";
import { tokensTable, tradesTable, resetsTable } from "@workspace/db/schema";
import { sql, eq, and, isNull } from "drizzle-orm";
import {
  state,
  log,
  sendToProxy,
  CAPACITY_LIMITS,
  PER_PROVIDER_LIMIT,
  DETECTION_WINDOW,
  RESET_COOLDOWN,
} from "./stress-state.js";

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
          isNull(resetsTable.firstTradeAfterAt)
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

        await db.insert(tradesTable).values({
          mint: data.mint,
          provider: "test",
          signature: data.signature,
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
    // Evict oldest subscription (insertion-ordered Set)
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

  // Test provider
  if (state.testSubscriptions.size >= 10) {
    if (now - state.testLastTradeAt > DETECTION_WINDOW) {
      if (now - state.testLastResetAt > RESET_COOLDOWN) {
        state.testIsStalled = true;
        state.testLastResetAt = now;
        await db.insert(resetsTable).values({ provider: "test", resetTriggeredAt: now });
        log("[test] Stalled — resetting connection", "warn");
        state.testConnection?.close();
      }
    } else {
      state.testIsStalled = false;
    }
  }

  // Proxy providers
  for (const [proxyId, proxy] of state.proxies) {
    if (proxy.subscriptions.size < 10) continue;
    if (now - proxy.lastTradeAt > DETECTION_WINDOW) {
      if (now - proxy.lastResetAt > RESET_COOLDOWN) {
        proxy.isStalled = true;
        proxy.lastResetAt = now;
        await db.insert(resetsTable).values({ provider: proxyId, resetTriggeredAt: now });
        log(`[${proxy.name}] Stalled — sending reset`, "warn");
        sendToProxy(proxyId, { type: "reset" });
      }
    } else {
      proxy.isStalled = false;
    }
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
  state.testSubscriptions = new Set();
  state.testLastTradeAt = Date.now();
  state.testIsStalled = false;
  state.testLastResetAt = 0;

  for (const proxy of state.proxies.values()) {
    proxy.subscriptions = new Set();
    proxy.isStalled = false;
    proxy.lastResetAt = 0;
    proxy.lastTradeAt = Date.now();
  }

  const limit = CAPACITY_LIMITS[mode];
  log(`TEST STARTED (Mode: ${mode}, Limit: ${limit} tokens, Proxies: ${state.proxies.size})`);

  await db.execute(sql`TRUNCATE tokens, trades, resets`);

  connectTestPumpDev();
  await new Promise((resolve) => setTimeout(resolve, 1000));

  const detectionInterval = setInterval(() => detectStalls(), 1000);
  let tokenIndex = 0;

  const pumpPortalWs = new WebSocket("wss://pumpportal.fun/api/data");

  pumpPortalWs.on("open", () => {
    log("Connected to PumpPortal");
    pumpPortalWs.send(JSON.stringify({ method: "subscribeNewToken" }));
  });

  pumpPortalWs.on("message", async (raw: Buffer) => {
    if (!state.isRunning) {
      pumpPortalWs.close();
      return;
    }

    try {
      const data = JSON.parse(raw.toString());
      if (!data.mint) return;

      tokenIndex++;
      state.totalTokens = tokenIndex;

      // Build ordered provider list: "test" always index 0
      const allProviders = ["test", ...Array.from(state.proxies.keys())];

      let provider1: string;
      let provider2: string | null = null;

      if (mode === "SINGLE") {
        provider1 = "test";
      } else if (mode === "DUAL") {
        provider1 = "test";
        provider2 = allProviders[1] ?? null;
      } else {
        // TRIPLE: each token → 2 of 3 providers, round-robin
        // Each provider carries 2/3 of tokens (4950 subs each → 7425 unique tokens)
        const pool = allProviders.slice(0, 3);
        provider1 = pool[tokenIndex % pool.length];
        provider2 = pool[(tokenIndex + 1) % pool.length];
      }

      await db.insert(tokensTable).values({
        mint: data.mint,
        provider1,
        provider2,
        assignedAt: Date.now(),
      });

      await handleAtCapacity(data.mint, provider1);
      if (provider2 && provider2 !== provider1) {
        await handleAtCapacity(data.mint, provider2);
      }

      log(
        `Token ${tokenIndex}: ${data.mint.slice(0, 8)}... → ${provider1.slice(0, 8)}${provider2 ? ", " + provider2.slice(0, 8) : ""}`
      );

      if (state.totalTokens >= limit) {
        log(`Reached capacity limit (${state.totalTokens}/${limit} unique tokens)`);
        setTimeout(() => {
          state.isRunning = false;
          clearInterval(detectionInterval);
          pumpPortalWs.close();
          log("TEST COMPLETED");
        }, 5000);
      }
    } catch (e: unknown) {
      log(`Token processing error: ${(e as Error).message}`, "error");
    }
  });

  pumpPortalWs.on("error", (err) => {
    log(`PumpPortal error: ${err.message}`, "error");
    clearInterval(detectionInterval);
    state.isRunning = false;
  });

  pumpPortalWs.on("close", () => {
    log("PumpPortal closed", "warn");
    if (state.isRunning) {
      clearInterval(detectionInterval);
      state.isRunning = false;
    }
  });
}

export function stopTest() {
  state.isRunning = false;
  if (state.testConnection) state.testConnection.close();
  for (const proxyId of state.proxies.keys()) {
    sendToProxy(proxyId, { type: "reset" });
  }
  log("TEST STOPPED", "warn");
}
