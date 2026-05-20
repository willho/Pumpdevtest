import { Router } from "express";
import { db } from "@workspace/db";
import { tokensTable, tradesTable, resetsTable, migrationsTable } from "@workspace/db/schema";
import { sql, eq, or, desc } from "drizzle-orm";
import { state, log, PER_PROVIDER_LIMIT } from "../lib/stress-state.js";
import { startTest, stopTest } from "../lib/stress-engine.js";

const router = Router();

router.post("/test/start", (req, res) => {
  const sourceNewToken = req.body?.sourceNewToken !== false;
  const sourceMigration = req.body?.sourceMigration === true;

  if (state.isRunning) {
    res.status(400).json({ error: "Already running" });
    return;
  }

  startTest(sourceNewToken, sourceMigration).catch((e: Error) =>
    log(`Start error: ${e.message}`, "error")
  );
  res.json({ status: "started", sourceNewToken, sourceMigration });
});

router.post("/test/stop", (_req, res) => {
  stopTest();
  res.json({ status: "stopped" });
});

router.get("/test/status", (_req, res) => {
  const rs = state.reconnectStats;
  const ts = state.tradeResumeStats;

  const proxies = Array.from(state.proxies.values()).map((p) => ({
    id: p.id,
    name: p.name,
    version: p.version,
    capacity: p.capacity,
    subscriptions: p.subscriptions.size,
    isStalled: p.isStalled,
    lastTradeAt: p.lastTradeAt,
    connectedAt: p.connectedAt,
  }));

  const capacityLimit = (1 + state.proxies.size) * PER_PROVIDER_LIMIT;

  res.json({
    isRunning: state.isRunning,
    sourceNewToken: state.sourceNewToken,
    sourceMigration: state.sourceMigration,
    totalTokens: state.totalTokens,
    totalTrades: state.totalTrades,
    totalMigrations: state.totalMigrations,
    nonPumpSwapMigrations: state.nonPumpSwapMigrations,
    uniqueWalletsCount: state.uniqueWallets.size,
    testStartAt: state.testStartAt,
    subscriptionsCount: state.subscriptionsCount,
    capacityLimit,
    rotationCount: state.rotationCount,
    testIsStalled: state.testIsStalled,
    testSubscriptions: state.testSubscriptions.size,
    simultaneousStall: state.simultaneousStall,
    proxies,
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

    const sources = [
      state.sourceNewToken ? "NEW_TOKEN" : null,
      state.sourceMigration ? "MIGRATION" : null,
    ].filter(Boolean).join("+") || "NONE";

    const proxyLines = Array.from(state.proxies.values())
      .map(
        (p) =>
          `  ${p.name} (${p.id.slice(0, 8)}): ${p.subscriptions.size}/${p.capacity} subs`
      )
      .join("\n");

    const capacityLimit = (1 + state.proxies.size) * PER_PROVIDER_LIMIT;

    const report = `
╔════════════════════════════════════════════════════════════════╗
║         STRESS TEST COORDINATOR: FINAL REPORT                  ║
╚════════════════════════════════════════════════════════════════╝

TEST SUMMARY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Mint Sources:             ${sources}
Capacity Limit:           ${capacityLimit} unique tokens
Total Tokens Discovered:  ${tokensCount}
Total Trades Captured:    ${tradesCount}
Total Resets Triggered:   ${resetsCount}
Total Migrations:         ${state.totalMigrations}
Connected Proxies:        ${state.proxies.size}

PROVIDER BREAKDOWN
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  test (coordinator):     ${state.testSubscriptions.size}/4950 subs
${proxyLines || "  (no external proxies)"}

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
Capacity limit held:      ${state.subscriptionsCount <= capacityLimit ? "YES" : "NO"}
Rotations handled:        ${state.rotationCount} tokens cycled
Reset resilience:         ${ts.all.length} resets with avg ${tsAvg}ms trade resume

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`;

    res.json({ report });
  } catch (e: unknown) {
    res.status(500).json({ error: (e as Error).message });
  }
});

router.post("/test/reset-db", async (_req, res) => {
  if (state.isRunning) {
    res.status(409).json({ error: "Cannot reset while test is running" });
    return;
  }
  try {
    await db.execute(sql`TRUNCATE tokens, trades, resets, migrations, rotations`);
    log("DB reset by user");
    res.json({ status: "ok" });
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
        or(
          eq(tokensTable.provider1, provider),
          eq(tokensTable.provider2, provider)
        )
      )
      .limit(200);

    res.json(rows);
  } catch (e: unknown) {
    res.status(500).json({ error: (e as Error).message });
  }
});

// ---------------------------------------------------------------------------
// DB inspection endpoints
// ---------------------------------------------------------------------------

router.get("/db/summary", async (_req, res) => {
  try {
    const [tokensCount, tradesCount, resetsCount, migrationsCount] = await Promise.all([
      db.$count(tokensTable),
      db.$count(tradesTable),
      db.$count(resetsTable),
      db.$count(migrationsTable),
    ]);
    res.json({ tokens: tokensCount, trades: tradesCount, resets: resetsCount, migrations: migrationsCount });
  } catch (e: unknown) {
    res.status(500).json({ error: (e as Error).message });
  }
});

router.get("/db/tokens", async (req, res) => {
  try {
    const limit = Math.min(Number(req.query["limit"] ?? 200), 1000);
    const offset = Number(req.query["offset"] ?? 0);
    const rows = await db
      .select()
      .from(tokensTable)
      .orderBy(desc(tokensTable.assignedAt))
      .limit(limit)
      .offset(offset);
    res.json(rows);
  } catch (e: unknown) {
    res.status(500).json({ error: (e as Error).message });
  }
});

router.get("/db/trades", async (req, res) => {
  try {
    const limit = Math.min(Number(req.query["limit"] ?? 200), 1000);
    const offset = Number(req.query["offset"] ?? 0);
    const mint = req.query["mint"] as string | undefined;
    const query = db.select().from(tradesTable);
    const rows = await (mint
      ? query.where(eq(tradesTable.mint, mint)).orderBy(desc(tradesTable.receivedAt)).limit(limit).offset(offset)
      : query.orderBy(desc(tradesTable.receivedAt)).limit(limit).offset(offset));
    res.json(rows);
  } catch (e: unknown) {
    res.status(500).json({ error: (e as Error).message });
  }
});

router.get("/db/resets", async (_req, res) => {
  try {
    const rows = await db.select().from(resetsTable).orderBy(desc(resetsTable.resetTriggeredAt)).limit(200);
    res.json(rows);
  } catch (e: unknown) {
    res.status(500).json({ error: (e as Error).message });
  }
});

router.get("/db/migrations", async (req, res) => {
  try {
    const limit = Math.min(Number(req.query["limit"] ?? 200), 1000);
    const offset = Number(req.query["offset"] ?? 0);
    const rows = await db.select().from(migrationsTable).orderBy(desc(migrationsTable.detectedAt)).limit(limit).offset(offset);
    res.json(rows);
  } catch (e: unknown) {
    res.status(500).json({ error: (e as Error).message });
  }
});

export default router;
