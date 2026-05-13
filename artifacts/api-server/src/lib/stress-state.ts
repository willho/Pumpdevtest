export interface LatencyStats {
  best: number;
  worst: number;
  all: number[];
}

export interface StressState {
  isRunning: boolean;
  mode: "SINGLE" | "DUAL" | "TRIPLE" | null;
  testConnection: import("ws").WebSocket | null;
  totalTokens: number;
  totalTrades: number;
  subscriptionsCount: number;
  connectedProviders: Set<string>;
  stalledProviders: Set<string>;
  resetTimestamps: Record<string, number>;
  rotationCount: number;
  reconnectStats: LatencyStats;
  tradeResumeStats: LatencyStats;
  logs: string[];
}

export const state: StressState = {
  isRunning: false,
  mode: null,
  testConnection: null,
  totalTokens: 0,
  totalTrades: 0,
  subscriptionsCount: 0,
  connectedProviders: new Set(["test"]),
  stalledProviders: new Set(),
  resetTimestamps: { test: 0, proxy2: 0, proxy3: 0 },
  rotationCount: 0,
  reconnectStats: { best: Infinity, worst: 0, all: [] },
  tradeResumeStats: { best: Infinity, worst: 0, all: [] },
  logs: [],
};

export const CAPACITY_LIMITS: Record<string, number> = {
  SINGLE: 4950,
  DUAL: 4950,
  TRIPLE: 7425,
};

export const DETECTION_WINDOW = 5000;
export const CHECK_INTERVAL = 1000;
export const RESET_COOLDOWN = 30000;

export function log(message: string, level: "info" | "warn" | "error" = "info") {
  const ts = new Date().toISOString();
  const entry = `[${ts}] [${level.toUpperCase()}] ${message}`;
  state.logs.push(entry);
  if (state.logs.length > 500) state.logs = state.logs.slice(-500);
  console.log(entry);
}
