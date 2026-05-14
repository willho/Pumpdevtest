import WebSocket from "ws";

export interface LatencyStats {
  best: number;
  worst: number;
  all: number[];
}

export interface ProxyInfo {
  id: string;
  name: string;
  version: string;
  capacity: number;
  subscriptions: Set<string>;
  lastTradeAt: number;
  connectedAt: number;
  isStalled: boolean;
  lastResetAt: number;
}

export interface StressState {
  isRunning: boolean;
  mode: "SINGLE" | "DUAL" | "TRIPLE" | null;
  testConnection: WebSocket | null;
  testSubscriptions: Set<string>;
  testLastTradeAt: number;
  testIsStalled: boolean;
  testLastResetAt: number;
  proxies: Map<string, ProxyInfo>;
  proxyWs: Map<string, WebSocket>;
  totalTokens: number;
  totalTrades: number;
  subscriptionsCount: number;
  rotationCount: number;
  reconnectStats: LatencyStats;
  tradeResumeStats: LatencyStats;
  logs: string[];
}

export const state: StressState = {
  isRunning: false,
  mode: null,
  testConnection: null,
  testSubscriptions: new Set(),
  testLastTradeAt: 0,
  testIsStalled: false,
  testLastResetAt: 0,
  proxies: new Map(),
  proxyWs: new Map(),
  totalTokens: 0,
  totalTrades: 0,
  subscriptionsCount: 0,
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

export const PER_PROVIDER_LIMIT = 4950;
export const DETECTION_WINDOW = 5000;
export const CHECK_INTERVAL = 1000;
export const RESET_COOLDOWN = 30000;

export function sendToProxy(
  proxyId: string,
  msg: Record<string, unknown>
): boolean {
  const ws = state.proxyWs.get(proxyId);
  if (!ws || ws.readyState !== WebSocket.OPEN) return false;
  ws.send(JSON.stringify(msg));
  return true;
}

export function log(
  message: string,
  level: "info" | "warn" | "error" = "info"
) {
  const ts = new Date().toISOString();
  const entry = `[${ts}] [${level.toUpperCase()}] ${message}`;
  state.logs.push(entry);
  if (state.logs.length > 500) state.logs = state.logs.slice(-500);
  console.log(entry);
}
