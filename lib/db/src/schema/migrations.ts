import { pgTable, serial, varchar, bigint, numeric, timestamp } from "drizzle-orm/pg-core";

export const migrationsTable = pgTable("migrations", {
  id: serial("id").primaryKey(),
  mint: varchar("mint", { length: 255 }).notNull().unique(),
  poolAddress: varchar("pool_address", { length: 255 }).notNull(),
  signature: varchar("signature", { length: 255 }).notNull(),
  detectedAt: bigint("detected_at", { mode: "number" }).notNull(),
  mintAmount: numeric("mint_amount").notNull(),
  solAmount: numeric("sol_amount").notNull(),
  provider: varchar("provider", { length: 50 }).notNull(),
  createdAt: timestamp("created_at").defaultNow(),
});

export type Migration = typeof migrationsTable.$inferSelect;

export const rotationsTable = pgTable("rotations", {
  id: serial("id").primaryKey(),
  mint: varchar("mint", { length: 255 }).notNull(),
  discoveredAt: bigint("discovered_at", { mode: "number" }).notNull(),
  graduatedAt: bigint("graduated_at", { mode: "number" }),
  lastTradeAt: bigint("last_trade_at", { mode: "number" }).notNull(),
  rotatedAt: bigint("rotated_at", { mode: "number" }).notNull(),
  timeSinceLastTradeMs: bigint("time_since_last_trade_ms", { mode: "number" }).notNull(),
  ageMs: bigint("age_ms", { mode: "number" }).notNull(),
  uniqueBuyers: numeric("unique_buyers").notNull(),
  totalVolumeSol: numeric("total_volume_sol").notNull(),
  createdAt: timestamp("created_at").defaultNow(),
});

export type Rotation = typeof rotationsTable.$inferSelect;
