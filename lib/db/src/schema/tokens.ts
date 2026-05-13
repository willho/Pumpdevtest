import { pgTable, serial, varchar, bigint, timestamp } from "drizzle-orm/pg-core";

export const tokensTable = pgTable("tokens", {
  id: serial("id").primaryKey(),
  mint: varchar("mint", { length: 255 }).notNull().unique(),
  provider1: varchar("provider1", { length: 50 }).notNull(),
  provider2: varchar("provider2", { length: 50 }),
  assignedAt: bigint("assigned_at", { mode: "number" }).notNull(),
  subscribedAt: bigint("subscribed_at", { mode: "number" }),
  unsubscribedAt: bigint("unsubscribed_at", { mode: "number" }),
  createdAt: timestamp("created_at").defaultNow(),
});

export type Token = typeof tokensTable.$inferSelect;
