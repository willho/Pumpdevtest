/**
 * Real Migration Detection Engine
 *
 * Connects to Chainstack logsSubscribe for actual Pump.fun migration events.
 * Detects token graduations (bonding curve → PumpSwap) in real-time.
 * Tracks rotations with actual time-since-last-trade metrics.
 */

import WebSocket from "ws";
import { PublicKey } from "@solana/web3.js";
import { db } from "@workspace/db";
import { migrationsTable, rotationsTable, tokensTable, tradesTable } from "@workspace/db/schema";
import { eq, sql } from "drizzle-orm";
import { state, log } from "./stress-state.js";

const PUMP_FUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const STALL_THRESHOLD = 30000;
const ROTATION_CAPACITY = 4950;

interface MigrationProvider {
  name: string;
  url: string;
}

interface MigrationState {
  providers: Map<string, WebSocket | null>;
  reconnectAttempts: Map<string, number>;
  pingIntervals: Map<string, NodeJS.Timeout>;
  lastEventAt: Map<string, number>;
  maxReconnectAttempts: number;
  baseBackoffMs: number;
  isRunning: boolean;
}

const migrationState: MigrationState = {
  providers: new Map(),
  reconnectAttempts: new Map(),
  pingIntervals: new Map(),
  lastEventAt: new Map(),
  maxReconnectAttempts: 10,
  baseBackoffMs: 1000,
  isRunning: false,
};

/**
 * Parse CompletePumpAmmMigrationEvent from log data
 * Offsets: mint (32-63), pool (128-159), mintAmount (64-71), solAmount (72-79)
 */
function parseMigrationEvent(
  logs: string[],
  _signature: string
): {
  mint: string;
  poolAddress: string;
  mintAmount: bigint;
  solAmount: bigint;
} | null {
  try {
    const programDataLog = logs.find((l) => l.includes("Program data:"));
    if (!programDataLog) return null;

    const match = programDataLog.match(/Program data: \[(.*?)\]/);
    if (!match) return null;

    const base64Data = match[1].split(",").map((s) => parseInt(s.trim()));

    if (base64Data.length < 160) return null;

    const mintBytes = Buffer.from(base64Data.slice(32, 64));
    const poolBytes = Buffer.from(base64Data.slice(128, 160));
    const mintAmountBytes = Buffer.from(base64Data.slice(64, 72));
    const solAmountBytes = Buffer.from(base64Data.slice(72, 80));

    const mintAmount = mintAmountBytes.readBigUInt64LE(0);
    const solAmount = solAmountBytes.readBigUInt64LE(0);

    const mint = new PublicKey(mintBytes).toBase58();
    const poolAddress = new PublicKey(poolBytes).toBase58();

    return { mint, poolAddress, mintAmount, solAmount };
  } catch (error) {
    log(`[migration] Parse error: ${(error as Error).message}`, "warn");
    return null;
  }
}

/**
 * Connect to Chainstack logsSubscribe provider
 */
function connectProvider(provider: MigrationProvider, offsetMinutes: number) {
  const existing = migrationState.providers.get(provider.name);
  if (existing) {
    existing.removeAllListeners();
    existing.close();
  }

  const pingInterval = migrationState.pingIntervals.get(provider.name);
  if (pingInterval) {
    clearInterval(pingInterval);
  }

  migrationState.reconnectAttempts.set(provider.name, 0);
  migrationState.lastEventAt.set(provider.name, Date.now());

  try {
    const ws = new WebSocket(provider.url);
    migrationState.providers.set(provider.name, ws);

    ws.on("open", () => {
      log(`[migration] ${provider.name} connected to Chainstack`);
      migrationState.reconnectAttempts.set(provider.name, 0);

      ws.send(
        JSON.stringify({
          jsonrpc: "2.0",
          id: 1,
          method: "logsSubscribe",
          params: [
            { mentions: [PUMP_FUN_PROGRAM] },
            { commitment: "processed" },
          ],
        })
      );

      const offsetMs = offsetMinutes * 60 * 1000;
      setTimeout(() => {
        if (ws.readyState === 1) {
          const interval = setInterval(() => {
            if (ws && ws.readyState === 1) {
              ws.ping();
            }
          }, 30 * 60 * 1000);
          migrationState.pingIntervals.set(provider.name, interval);
        }
      }, offsetMs);
    });

    ws.on("message", async (raw: Buffer) => {
      if (!migrationState.isRunning) return;

      try {
        const msg = JSON.parse(raw.toString());

        if (msg.result && typeof msg.result === "string") {
          log(`[migration] ${provider.name} subscribed`);
          return;
        }

        if (msg.params?.result?.value?.logs) {
          const logs = msg.params.result.value.logs;
          const signature = msg.params.result.value.signature;

          const hasMigration = logs.some((l: string) =>
            l.includes("Instruction: Migrate")
          );

          if (hasMigration) {
            migrationState.lastEventAt.set(provider.name, Date.now());

            const migration = parseMigrationEvent(logs, signature);
            if (migration) {
              await db.insert(migrationsTable).values({
                mint: migration.mint,
                poolAddress: migration.poolAddress,
                signature,
                detectedAt: Date.now(),
                mintAmount: migration.mintAmount.toString(),
                solAmount: migration.solAmount.toString(),
                provider: provider.name,
              });

              state.totalMigrations++;
              state.migrationProviderLastEventAt.set(provider.name, Date.now());
              state.migrationProvidersStalled.delete(provider.name);

              await checkTokenRotation(migration.mint);

              log(
                `[migration] Graduated: ${migration.mint.slice(0, 8)}... via ${provider.name}`
              );
            }
          }
        }
      } catch (_error) {
        // Silent parse error
      }
    });

    ws.on("close", (code, reason) => {
      log(
        `[migration] ${provider.name} closed (code ${code}): ${reason || "no reason"}`,
        "warn"
      );

      const interval = migrationState.pingIntervals.get(provider.name);
      if (interval) {
        clearInterval(interval);
        migrationState.pingIntervals.delete(provider.name);
      }

      if (migrationState.isRunning) {
        setTimeout(() => {
          log(`[migration] ${provider.name} reconnecting...`);
          connectProvider(provider, offsetMinutes);
        }, 2000);
      }
    });

    ws.on("error", (err: Error) => {
      log(`[migration] ${provider.name} error: ${err.message}`, "error");
    });
  } catch (error: unknown) {
    log(
      `[migration] ${provider.name} connection error: ${(error as Error).message}`,
      "error"
    );

    if (migrationState.isRunning) {
      setTimeout(() => {
        connectProvider(provider, offsetMinutes);
      }, 2000);
    }
  }
}

/**
 * Check if token rotation needed and record metrics
 */
async function checkTokenRotation(mint: string) {
  try {
    const allTokens = await db.select().from(tokensTable);

    if (allTokens.length <= ROTATION_CAPACITY) return;

    const slowestToken = await db
      .select({ mint: tokensTable.mint, assignedAt: tokensTable.assignedAt })
      .from(tokensTable)
      .orderBy(tokensTable.assignedAt)
      .limit(1);

    if (slowestToken.length === 0) return;

    const tokenToRotate = slowestToken[0];
    const now = Date.now();

    const lastTrade = await db
      .select()
      .from(tradesTable)
      .where(eq(tradesTable.mint, tokenToRotate.mint))
      .orderBy(sql`${tradesTable.receivedAt} DESC`)
      .limit(1);

    // FIX: was `lastTradeAt` (undefined) in original PR
    const lastTradeTime =
      lastTrade.length > 0
        ? lastTrade[0].receivedAt
        : tokenToRotate.assignedAt;
    const timeSinceLastTrade = now - lastTradeTime;

    const buyerRows = await db
      .select({ wallet: tradesTable.wallet })
      .from(tradesTable)
      .where(eq(tradesTable.mint, tokenToRotate.mint));

    const uniqueBuyers = new Set(
      buyerRows.map((t) => t.wallet).filter(Boolean)
    ).size;

    const volumeData = await db
      .select({ volume: sql`COALESCE(COUNT(*), 0)` })
      .from(tradesTable)
      .where(eq(tradesTable.mint, tokenToRotate.mint));

    const volume =
      volumeData.length > 0 ? Number(volumeData[0].volume) : 0;

    const migration = await db
      .select()
      .from(migrationsTable)
      .where(eq(migrationsTable.mint, tokenToRotate.mint))
      .limit(1);

    const graduatedAt =
      migration.length > 0 ? migration[0].detectedAt : undefined;

    await db.insert(rotationsTable).values({
      mint: tokenToRotate.mint,
      discoveredAt: tokenToRotate.assignedAt,
      graduatedAt: graduatedAt ?? null,
      lastTradeAt: lastTradeTime,
      rotatedAt: now,
      timeSinceLastTradeMs: timeSinceLastTrade,
      ageMs: now - tokenToRotate.assignedAt,
      uniqueBuyers: uniqueBuyers.toString(),
      totalVolumeSol: volume.toString(),
    });

    state.totalRotations++;

    await db.delete(tokensTable).where(eq(tokensTable.mint, tokenToRotate.mint));

    log(
      `[rotation] Rotated ${tokenToRotate.mint.slice(0, 8)}... (${Math.round(timeSinceLastTrade / 1000)}s inactive, ${uniqueBuyers} buyers)`
    );
  } catch (error) {
    log(`[rotation] Error: ${(error as Error).message}`, "error");
  }
}

/**
 * Start migration detection with Chainstack providers
 */
export async function startMigrationDetection(
  providers: MigrationProvider[]
): Promise<void> {
  if (providers.length === 0) {
    log("[migration] No providers configured", "warn");
    return;
  }

  migrationState.isRunning = true;
  log(`[migration] Starting with ${providers.length} provider(s)`);

  const totalProviders = providers.length;
  for (let i = 0; i < providers.length; i++) {
    const offsetMin = Math.floor((i * 30) / totalProviders);
    log(`[migration] ${providers[i].name}: offset ${offsetMin}min`);
    migrationState.lastEventAt.set(providers[i].name, Date.now());
    connectProvider(providers[i], offsetMin);
  }

  const stallCheckInterval = setInterval(() => {
    if (!migrationState.isRunning) {
      clearInterval(stallCheckInterval);
      return;
    }

    const now = Date.now();
    let anyActive = false;

    for (const [, lastEventTime] of migrationState.lastEventAt) {
      if (now - lastEventTime < STALL_THRESHOLD) {
        anyActive = true;
      }
    }

    if (!anyActive && migrationState.lastEventAt.size > 0) {
      log("[migration] ALL PROVIDERS STALLED - critical event", "error");
    }
  }, 5000);
}

/**
 * Stop migration detection
 */
export async function stopMigrationDetection(): Promise<void> {
  migrationState.isRunning = false;

  for (const ws of migrationState.providers.values()) {
    if (ws) ws.close();
  }

  for (const interval of migrationState.pingIntervals.values()) {
    clearInterval(interval);
  }

  log("[migration] Stopped");
}

/**
 * Get migration stats
 */
export async function getMigrationStats() {
  const totalMigrations = await db.select().from(migrationsTable);
  const totalRotations = await db.select().from(rotationsTable);

  const rotationTimes = totalRotations.map(
    (r) => r.timeSinceLastTradeMs as unknown as number
  );
  const avgRotationTime =
    rotationTimes.length > 0
      ? rotationTimes.reduce((a, b) => a + b, 0) / rotationTimes.length
      : 0;

  return {
    totalMigrations: totalMigrations.length,
    totalRotations: totalRotations.length,
    avgTimeSinceLastTradeMs: avgRotationTime,
    providerStatus: Array.from(migrationState.lastEventAt.entries()).map(
      ([name, lastTime]) => ({
        name,
        isActive: Date.now() - lastTime < STALL_THRESHOLD,
        lastEventAgo: Math.floor((Date.now() - lastTime) / 1000),
      })
    ),
  };
}
