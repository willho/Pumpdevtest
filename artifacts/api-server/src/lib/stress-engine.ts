import WebSocket from "ws";
import { db } from "@workspace/db";
import {
  tokensTable,
  tradesTable,
  resetsTable,
} from "@workspace/db/schema";
import { sql, eq, and, isNull, inArray, gt } from "drizzle-orm";
import {
  state,
  log,
  CAPACITY_LIMITS,
  DETECTION_WINDOW,
  RESET_COOLDOWN,
} from "./stress-state.js";

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
        .set({
          firstTradeAfterAt: tradeTime,
          tradeResumeLatencyMs: resumeLatency,
        })
        .where(eq(resetsTable.id, reset.id));

      state.tradeResumeStats.all.push(resumeLatency);
      state.tradeResumeStats.best = Math.min(
        state.tradeResumeStats.best,
        resumeLatency
      );
      state.tradeResumeStats.worst = Math.max(
        state.tradeResumeStats.worst,
        resumeLatency
      );
    }
  } catch (e: unknown) {
    log(`Resume check error: ${(e as Error).message}`, "error");
  }
}

export function connectTestPumpDev() {
  const wsUrl = "wss://pumpdev.io/ws";
  let ws: WebSocket;

  try {
    ws = new WebSocket(wsUrl);
    state.testConnection = ws;

    ws.on("open", () => {
      log("[test] Connected to PumpDev");
    });

    ws.on("message", async (raw: Buffer) => {
      try {
        const data = JSON.parse(raw.toString());
        if (data.error || !data.signature) return;

        state.totalTrades++;
        const now = Date.now();

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
      log("[test] Connection closed - reconnecting in 2s", "warn");
      setTimeout(() => connectTestPumpDev(), 2000);
    });
  } catch (e: unknown) {
    log(`[test] Failed to connect: ${(e as Error).message}`, "error");
  }
}

async function subscribeTokens(mints: string[]) {
  const ws = state.testConnection;
  if (!ws || ws.readyState !== WebSocket.OPEN) return false;
  if (mints.length === 0) return false;

  try {
    ws.send(
      JSON.stringify({ method: "subscribeTokenTrade", keys: mints })
    );

    const now = Date.now();
    await db
      .update(tokensTable)
      .set({ subscribedAt: now })
      .where(inArray(tokensTable.mint, mints));

    state.subscriptionsCount += mints.length;
    return true;
  } catch (e: unknown) {
    log(`Failed to subscribe: ${(e as Error).message}`, "error");
    return false;
  }
}

async function unsubscribeToken(mint: string) {
  const ws = state.testConnection;
  if (!ws || ws.readyState !== WebSocket.OPEN) return false;

  try {
    ws.send(
      JSON.stringify({ method: "unsubscribeTokenTrade", keys: [mint] })
    );

    const now = Date.now();
    await db
      .update(tokensTable)
      .set({ unsubscribedAt: now })
      .where(eq(tokensTable.mint, mint));

    state.subscriptionsCount--;
    state.rotationCount++;
    return true;
  } catch (e: unknown) {
    log(`Failed to unsubscribe: ${(e as Error).message}`, "error");
    return false;
  }
}

export async function handleTokenAtCapacity(mint: string, provider: string) {
  const limit = CAPACITY_LIMITS[state.mode!];

  if (state.subscriptionsCount >= limit) {
    const result = await db
      .select({ mint: tokensTable.mint })
      .from(tokensTable)
      .where(
        and(
          eq(tokensTable.provider1, provider),
          sql`${tokensTable.subscribedAt} IS NOT NULL`,
          isNull(tokensTable.unsubscribedAt)
        )
      )
      .orderBy(tokensTable.subscribedAt)
      .limit(1);

    if (result.length > 0) {
      const oldestMint = result[0].mint;
      await unsubscribeToken(oldestMint);
      await subscribeTokens([mint]);
      log(
        `Rotation: unsubscribed ${oldestMint.slice(0, 8)}..., subscribed ${mint.slice(0, 8)}...`
      );
      return;
    }
  }

  await subscribeTokens([mint]);
}

export async function detectStalls() {
  if (!state.isRunning || !state.mode) return;

  try {
    const now = Date.now();
    const windowStart = now - DETECTION_WINDOW;
    const providers = Array.from(state.connectedProviders);

    const tokenRows = await db
      .select()
      .from(tokensTable)
      .where(inArray(tokensTable.provider1, providers))
      .limit(1000);

    const expectedProviders: Record<string, string[]> = {};
    const missingProviders: Record<string, Set<string>> = {};

    for (const token of tokenRows) {
      const ps: string[] = [];
      if (state.connectedProviders.has(token.provider1))
        ps.push(token.provider1);
      if (token.provider2 && state.connectedProviders.has(token.provider2))
        ps.push(token.provider2);
      expectedProviders[token.mint] = ps;
      missingProviders[token.mint] = new Set(ps);
    }

    const tradeRows = await db
      .select({ mint: tradesTable.mint, provider: tradesTable.provider })
      .from(tradesTable)
      .where(gt(tradesTable.receivedAt, windowStart));

    for (const trade of tradeRows) {
      if (missingProviders[trade.mint]) {
        missingProviders[trade.mint].delete(trade.provider);
      }
    }

    const newStalled = new Set<string>();
    for (const [mint, missing] of Object.entries(missingProviders)) {
      for (const provider of missing) {
        if (expectedProviders[mint].includes(provider)) {
          newStalled.add(provider);
        }
      }
    }

    for (const provider of newStalled) {
      const timeSinceLastReset = now - (state.resetTimestamps[provider] ?? 0);
      if (timeSinceLastReset > RESET_COOLDOWN) {
        state.resetTimestamps[provider] = now;

        await db.insert(resetsTable).values({
          provider,
          resetTriggeredAt: now,
        });

        if (provider === "test") {
          log(`[test] Connection stalled - resetting`, "warn");
          state.testConnection?.close();
        } else {
          log(`Resetting ${provider}`, "warn");
        }
      }
    }

    state.stalledProviders = newStalled;
  } catch (e: unknown) {
    log(`Detection error: ${(e as Error).message}`, "error");
  }
}

export async function startTest(mode: "SINGLE" | "DUAL" | "TRIPLE") {
  if (state.isRunning) return;

  state.isRunning = true;
  state.mode = mode;
  state.logs = [];
  state.subscriptionsCount = 0;
  state.rotationCount = 0;
  state.stalledProviders.clear();
  state.resetTimestamps = { test: 0, proxy2: 0, proxy3: 0 };
  state.reconnectStats = { best: Infinity, worst: 0, all: [] };
  state.tradeResumeStats = { best: Infinity, worst: 0, all: [] };
  state.totalTokens = 0;
  state.totalTrades = 0;

  const limit = CAPACITY_LIMITS[mode];
  log(`TEST STARTED (Mode: ${mode}, Limit: ${limit} tokens)`);

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

      let provider1: string;
      let provider2: string | null = null;

      if (mode === "SINGLE") {
        provider1 = "test";
      } else if (mode === "DUAL") {
        const connected = Array.from(state.connectedProviders);
        provider1 = connected[tokenIndex % connected.length];
        provider2 = connected[(tokenIndex + 1) % connected.length];
      } else {
        provider1 = ["test", "proxy2", "proxy3"][tokenIndex % 3];
        provider2 = ["test", "proxy2", "proxy3"][(tokenIndex + 1) % 3];
      }

      await db.insert(tokensTable).values({
        mint: data.mint,
        provider1,
        provider2,
        assignedAt: Date.now(),
      });

      if (provider1 === "test" || provider2 === "test") {
        await handleTokenAtCapacity(data.mint, "test");
      }

      log(
        `Token ${tokenIndex}: ${data.mint.slice(0, 8)}... → ${provider1}${provider2 ? ", " + provider2 : ""}`
      );

      if (state.subscriptionsCount >= limit) {
        log(`Reached capacity limit (${state.subscriptionsCount}/${limit})`);
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
  log("TEST STOPPED", "warn");
}
