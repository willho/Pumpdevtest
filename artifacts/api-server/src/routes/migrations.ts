import { Router } from "express";
import { db } from "@workspace/db";
import { migrationsTable, rotationsTable } from "@workspace/db/schema";
import { desc } from "drizzle-orm";
import { state, log } from "../lib/stress-state.js";

const router = Router();

router.get("/migrations/stats", async (_req, res) => {
  try {
    const [migrationCount, rotationCount] = await Promise.all([
      db.$count(migrationsTable),
      db.$count(rotationsTable),
    ]);

    const recentRotations = await db
      .select({
        mint: rotationsTable.mint,
        rotatedAt: rotationsTable.rotatedAt,
        timeSinceLastTradeMs: rotationsTable.timeSinceLastTradeMs,
        ageMs: rotationsTable.ageMs,
        uniqueBuyers: rotationsTable.uniqueBuyers,
        totalVolumeSol: rotationsTable.totalVolumeSol,
      })
      .from(rotationsTable)
      .orderBy(desc(rotationsTable.rotatedAt))
      .limit(20);

    res.json({
      totalMigrations: state.totalMigrations,
      totalRotations: state.totalRotations,
      databaseMigrationCount: migrationCount,
      databaseRotationCount: rotationCount,
      migrationProviders: {
        active: Array.from(state.migrationProviderLastEventAt.keys()),
        stalled: Array.from(state.migrationProvidersStalled),
        lastEventTimes: Object.fromEntries(
          state.migrationProviderLastEventAt
        ),
      },
      recentRotations,
      timestamp: Date.now(),
    });
  } catch (e: unknown) {
    log(`Migration stats error: ${(e as Error).message}`, "error");
    res.status(500).json({ error: (e as Error).message });
  }
});

router.get("/migrations/detailed", async (_req, res) => {
  try {
    const migrations = await db
      .select()
      .from(migrationsTable)
      .orderBy(desc(migrationsTable.detectedAt))
      .limit(100);

    const rotations = await db
      .select()
      .from(rotationsTable)
      .orderBy(desc(rotationsTable.rotatedAt))
      .limit(100);

    res.json({
      migrations: migrations.map((m) => ({
        mint: m.mint,
        poolAddress: m.poolAddress,
        signature: m.signature,
        provider: m.provider,
        detectedAt: m.detectedAt,
        mintAmount: m.mintAmount,
        solAmount: m.solAmount,
      })),
      rotations: rotations.map((r) => ({
        mint: r.mint,
        discoveredAt: r.discoveredAt,
        graduatedAt: r.graduatedAt,
        lastTradeAt: r.lastTradeAt,
        rotatedAt: r.rotatedAt,
        timeSinceLastTradeMs: r.timeSinceLastTradeMs,
        ageMs: r.ageMs,
        uniqueBuyers: r.uniqueBuyers,
        totalVolumeSol: r.totalVolumeSol,
      })),
    });
  } catch (e: unknown) {
    log(`Detailed migrations error: ${(e as Error).message}`, "error");
    res.status(500).json({ error: (e as Error).message });
  }
});

export default router;
