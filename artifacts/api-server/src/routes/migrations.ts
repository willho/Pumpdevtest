import { Router } from "express";
import { db } from "@workspace/db";
import { rotationsTable } from "@workspace/db/schema";
import { desc } from "drizzle-orm";
import { state, log } from "../lib/stress-state.js";

const router = Router();

router.get("/migrations/stats", async (_req, res) => {
  try {
    const [rotationCount, recentRotations] = await Promise.all([
      db.$count(rotationsTable),
      db
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
        .limit(20),
    ]);

    res.json({
      totalRotations: state.totalRotations,
      databaseRotationCount: rotationCount,
      recentRotations,
      timestamp: Date.now(),
    });
  } catch (e: unknown) {
    log(`Rotation stats error: ${(e as Error).message}`, "error");
    res.status(500).json({ error: (e as Error).message });
  }
});

router.get("/migrations/detailed", async (_req, res) => {
  try {
    const rotations = await db
      .select()
      .from(rotationsTable)
      .orderBy(desc(rotationsTable.rotatedAt))
      .limit(100);

    res.json({
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
    log(`Detailed rotations error: ${(e as Error).message}`, "error");
    res.status(500).json({ error: (e as Error).message });
  }
});

export default router;
