import { pgTable, serial, varchar, bigint, integer, timestamp } from "drizzle-orm/pg-core";

export const resetsTable = pgTable("resets", {
  id: serial("id").primaryKey(),
  provider: varchar("provider", { length: 50 }).notNull(),
  resetTriggeredAt: bigint("reset_triggered_at", { mode: "number" }).notNull(),
  reconnectAt: bigint("reconnect_at", { mode: "number" }),
  firstTradeAfterAt: bigint("first_trade_after_at", { mode: "number" }),
  reconnectLatencyMs: integer("reconnect_latency_ms"),
  tradeResumeLatencyMs: integer("trade_resume_latency_ms"),
  createdAt: timestamp("created_at").defaultNow(),
});

export type Reset = typeof resetsTable.$inferSelect;
