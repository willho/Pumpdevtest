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
import { state, log, sendToProxy } from "./stress-state.js";

const PUMP_FUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const STALL_THRESHOLD = 30000; // 30 seconds
const ROTATION_CAPACITY = 4950; // Per provider

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
  signature: string
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
  // Clean up existing
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

      // Subscribe to Pump.fun migrations
      ws.send(
        JSON.stringify({
          jsonrpc: "2.0",
          id: 1,
          method: "logsSubscribe",
          params: [
            {
              mentions: [PUMP_FUN_PROGRAM],
            },
            {
              commitment: "processed",
            },
          ],
        })
      );

      // Offset ping (stagger pings to prevent collisions)
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

        // Subscription confirmation
        if (msg.result && typeof msg.result === "string") {
          log(`[migration] ${provider.name} subscribed`);
          return;
        }

        // Migration event
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
              // Record migration
              await db.insert(migrationsTable).values({
                mint: migration.mint,
                poolAddress: migration.poolAddress,
                signature,
                detectedAt: Math.floor(Date.now() / 1000),
                mintAmount: migration.mintAmount.toString(),
                solAmount: migration.solAmount.toString(),
                provider: provider.name,
              });

              // Check for rotation
              await checkTokenRotation(migration.mint);

              log(
                `[migration] Graduated: ${migration.mint.slice(0, 8)}... via ${provider.name}`
              );
            }
          } else if (logs.some((l: string) => l.includes("Error"))) {
            log(
              `[migration] Failed tx: ${signature.slice(0, 8)}...`,
              "warn"
            );
          }
        }
      } catch (error) {
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

      // Auto-reconnect with 2s backoff
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
  } catch (error: any) {
    log(
      `[migration] ${provider.name} connection error: ${error.message}`,
      "error"
    );

    // Retry with backoff
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
    // Get all tracked tokens
    const allTokens = await db.select().from(tokensTable);

    // Count rotations needed
    const rotationNeeded =
      allTokens.length > ROTATION_CAPACITY;

    if (!rotationNeeded) return;

    // Find slowest token (longest time since last trade)
    const slowestToken = await db
      .select({
        mint: tokensTable.mint,
        assignedAt: tokensTable.assignedAt,
      })
      .from(tokensTable)
      .orderBy(tokensTable.assignedAt)
      .limit(1);

    if (slowestToken.length === 0) return;

    const tokenToRotate = slowestToken[0];
    const now = Date.now();

    // Get last trade time for this token
    const lastTrade = await db
      .select()
      .from(tradesTable)
      .where(eq(tradesTable.mint, tokenToRotate.mint))
      .orderBy(sql`${tradesTable.receivedAt} DESC`)
      .limit(1);

    const lastTradeTime = lastTrade.length > 0 ? lastTrade[0].receivedAt * 1000 : tokenToRotate.assignedAt;
    const timeSinceLastTrade = now - lastTradeTime;

    // Count unique buyers
    const buyerCount = await db
      .select({ wallet: tradesTable.wallet })
      .from(tradesTable)
      .where(eq(tradesTable.mint, tokenToRotate.mint));

    const uniqueBuyers = new Set(buyerCount.map((t) => t.wallet).filter(Boolean)).size;

    // Calculate volume
    const volumeData = await db
      .select({ volume: sql`COALESCE(COUNT(*), 0)` })
      .from(tradesTable)
      .where(eq(tradesTable.mint, tokenToRotate.mint));

    const volume = volumeData.length > 0 ? Number(volumeData[0].volume) : 0;

    // Check if graduated
    const migration = await db
      .select()
      .from(migrationsTable)
      .where(eq(migrationsTable.mint, tokenToRotate.mint))
      .limit(1);

    const graduatedAt = migration.length > 0 ? migration[0].detectedAt * 1000 : undefined;

    // Record rotation
    await db.insert(rotationsTable).values({
      mint: tokenToRotate.mint,
      discoveredAt: tokenToRotate.assignedAt * 1000,
      graduatedAt: graduatedAt || null,
      lastTradeAt,
      rotatedAt: now,
      timeSinceLastTradeMs: timeSinceLastTrade,
      ageMs: now - (tokenToRotate.assignedAt * 1000),
      uniqueBuyers: uniqueBuyers.toString(),
      totalVolumeSol: volume.toString(),
    });

    // Remove from active tracking
    await db.delete(tokensTable).where(eq(tokensTable.mint, tokenToRotate.mint));

    log(
      `[rotation] Rotated ${tokenToRotate.mint.slice(0, 8)}... (${timeSinceLastTrade / 1000}s inactive, ${uniqueBuyers} buyers)`
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

  // Monitor stall detection
  const stallCheckInterval = setInterval(() => {
    if (!migrationState.isRunning) {
      clearInterval(stallCheckInterval);
      return;
    }

    const now = Date.now();
    let anyActive = false;

    for (const [name, lastEventTime] of migrationState.lastEventAt) {
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
    if (ws) {
      ws.close();
    }
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

  const rotationTimes = totalRotations.map((r) => r.timeSinceLastTradeMs as unknown as number);
  const avgRotationTime =
    rotationTimes.length > 0
      ? rotationTimes.reduce((a, b) => a + b, 0) / rotationTimes.length
      : 0;

  return {
    totalMigrations: totalMigrations.length,
    totalRotations: totalRotations.length,
    avgTimeSinceLastTradeMs: avgRotationTime,
    providerStatus: Array.from(migrationState.lastEventAt.entries()).map(([name, lastTime]) => ({
      name,
      isActive: Date.now() - lastTime < STALL_THRESHOLD,
      lastEventAgo: Math.floor((Date.now() - lastTime) / 1000),
    })),
  };
}
