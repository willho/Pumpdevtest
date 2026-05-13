import { Router } from "express";
import { db } from "@workspace/db";
import { tokensTable, tradesTable, resetsTable } from "@workspace/db/schema";
import { eq, or } from "drizzle-orm";
import {
  state,
  log,
  CAPACITY_LIMITS,
} from "../lib/stress-state.js";
import {
  startTest,
  stopTest,
  checkTradeResumeCompletion,
} from "../lib/stress-engine.js";

const router = Router();

router.post("/test/start/:mode", (req, res) => {
  const mode = (req.params["mode"] ?? "").toUpperCase() as
    | "SINGLE"
    | "DUAL"
    | "TRIPLE";

  if (!["SINGLE", "DUAL", "TRIPLE"].includes(mode)) {
    res.status(400).json({ error: "Invalid mode" });
    return;
  }

  if (state.isRunning) {
    res.status(400).json({ error: "Already running" });
    return;
  }

  startTest(mode).catch((e: Error) =>
    log(`Start error: ${e.message}`, "error")
  );
  res.json({ status: "started", mode });
});

router.post("/test/stop", (_req, res) => {
  stopTest();
  res.json({ status: "stopped" });
});

router.get("/test/status", (_req, res) => {
  const rs = state.reconnectStats;
  const ts = state.tradeResumeStats;

  res.json({
    isRunning: state.isRunning,
    mode: state.mode,
    totalTokens: state.totalTokens,
    totalTrades: state.totalTrades,
    subscriptionsCount: state.subscriptionsCount,
    capacityLimit: state.mode ? CAPACITY_LIMITS[state.mode] : 0,
    rotationCount: state.rotationCount,
    connectedProviders: Array.from(state.connectedProviders),
    reconnectStats: {
      best: rs.best === Infinity ? 0 : rs.best,
      worst: rs.worst,
      avg:
        rs.all.length > 0
          ? Math.round(rs.all.reduce((a, b) => a + b, 0) / rs.all.length)
          : 0,
      count: rs.all.length,
    },
    tradeResumeStats: {
      best: ts.best === Infinity ? 0 : ts.best,
      worst: ts.worst,
      avg:
        ts.all.length > 0
          ? Math.round(ts.all.reduce((a, b) => a + b, 0) / ts.all.length)
          : 0,
      count: ts.all.length,
    },
    logs: state.logs.slice(-50),
  });
});

router.get("/test/report", async (_req, res) => {
  try {
    const [tradesCount, tokensCount, resetsCount] = await Promise.all([
      db.$count(tradesTable),
      db.$count(tokensTable),
      db.$count(resetsTable),
    ]);

    const rs = state.reconnectStats;
    const ts = state.tradeResumeStats;
    const rsAvg =
      rs.all.length > 0
        ? Math.round(rs.all.reduce((a, b) => a + b, 0) / rs.all.length)
        : 0;
    const tsAvg =
      ts.all.length > 0
        ? Math.round(ts.all.reduce((a, b) => a + b, 0) / ts.all.length)
        : 0;

    const report = `
╔════════════════════════════════════════════════════════════════╗
║         STRESS TEST WITH ROTATION: FINAL REPORT                ║
╚════════════════════════════════════════════════════════════════╝

TEST SUMMARY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Mode:                     ${state.mode}
Capacity Limit (99%):     ${state.mode ? CAPACITY_LIMITS[state.mode] : "N/A"}
Total Tokens Discovered:  ${tokensCount}
Total Trades Captured:    ${tradesCount}
Total Resets Triggered:   ${resetsCount}

ROTATION PERFORMANCE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Total Rotations:          ${state.rotationCount}
Rotation Rate:            ${state.totalTokens > 0 ? ((state.rotationCount / state.totalTokens) * 100).toFixed(2) : 0}% of tokens

RECONNECT PERFORMANCE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Best Reconnect:           ${rs.best === Infinity ? "N/A" : rs.best + "ms"}
Worst Reconnect:          ${rs.worst}ms
Average Reconnect:        ${rsAvg}ms
Reconnections Measured:   ${rs.all.length}

TRADE RESUME LATENCY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Best Resume:              ${ts.best === Infinity ? "N/A" : ts.best + "ms"}
Worst Resume:             ${ts.worst}ms
Average Resume:           ${tsAvg}ms
Resumptions Measured:     ${ts.all.length}

CONCLUSIONS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Capacity limit held:      ${state.subscriptionsCount <= (state.mode ? CAPACITY_LIMITS[state.mode] : 0) ? "YES" : "NO"}
Rotations handled:        ${state.rotationCount} tokens cycled
Reset resilience:         ${rs.all.length} resets with avg ${rsAvg}ms reconnect

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`;

    res.json({ report });
  } catch (e: unknown) {
    res.status(500).json({ error: (e as Error).message });
  }
});

router.get("/tokens/:provider", async (req, res) => {
  try {
    const { provider } = req.params as { provider: string };
    const rows = await db
      .select({
        mint: tokensTable.mint,
        provider1: tokensTable.provider1,
        provider2: tokensTable.provider2,
      })
      .from(tokensTable)
      .where(
        or(eq(tokensTable.provider1, provider), eq(tokensTable.provider2, provider))
      )
      .limit(200);

    res.json(rows);
  } catch (e: unknown) {
    res.status(500).json({ error: (e as Error).message });
  }
});

router.post("/proxy/register", (req, res) => {
  const { provider } = req.body as { provider: string };
  state.connectedProviders.add(provider);
  log(`[Register] ${provider} connected`);
  res.json({ status: "registered" });
});

router.post("/proxy/reconnected", async (req, res) => {
  const { provider, resetTime } = req.body as {
    provider: string;
    resetTime: number;
  };
  const now = Date.now();
  const reconnectLatency = now - resetTime;

  state.reconnectStats.all.push(reconnectLatency);
  state.reconnectStats.best = Math.min(
    state.reconnectStats.best,
    reconnectLatency
  );
  state.reconnectStats.worst = Math.max(
    state.reconnectStats.worst,
    reconnectLatency
  );

  await db
    .update(resetsTable)
    .set({
      reconnectAt: now,
      reconnectLatencyMs: reconnectLatency,
    })
    .where(eq(resetsTable.resetTriggeredAt, resetTime));

  log(`[${provider}] Reconnected in ${reconnectLatency}ms`);
  res.json({ status: "ok" });
});

router.post("/trades", async (req, res) => {
  try {
    const { mint, provider, signature } = req.body as {
      mint: string;
      provider: string;
      signature: string;
    };
    const now = Date.now();

    await db.insert(tradesTable).values({
      mint,
      provider,
      signature,
      receivedAt: now,
    });

    state.totalTrades++;
    await checkTradeResumeCompletion(provider, now);
    res.json({ status: "ok" });
  } catch (e: unknown) {
    res.status(500).json({ error: (e as Error).message });
  }
});

export default router;
