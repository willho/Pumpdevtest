import { pgTable, serial, varchar, bigint, timestamp } from "drizzle-orm/pg-core";

export const tradesTable = pgTable("trades", {
  id: serial("id").primaryKey(),
  mint: varchar("mint", { length: 255 }).notNull(),
  provider: varchar("provider", { length: 50 }).notNull(),
  signature: varchar("signature", { length: 255 }).notNull(),
  receivedAt: bigint("received_at", { mode: "number" }).notNull(),
  createdAt: timestamp("created_at").defaultNow(),
});

export type Trade = typeof tradesTable.$inferSelect;
