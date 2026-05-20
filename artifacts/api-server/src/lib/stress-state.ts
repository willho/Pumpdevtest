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
  pumpPortalLastNewTokenAt: number;
  pumpPortalIsStalled: boolean;
}

export interface StressState {
  isRunning: boolean;
  mode: "RUNNING" | null;
  sourceNewToken: boolean;
  testConnection: WebSocket | null;
  testSubscriptions: Set<string>;
  testLastTradeAt: number;
  testIsStalled: boolean;
  testLastResetAt: number;
  proxies: Map<string, ProxyInfo>;
  proxyWs: Map<string, WebSocket>;
  totalTokens: number;
  totalTrades: number;
  totalRotations: number;
  subscriptionsCount: number;
  rotationCount: number;
  simultaneousStall: boolean;
  reconnectStats: LatencyStats;
  tradeResumeStats: LatencyStats;
  logs: string[];
  coordinatorLastNewTokenAt: number;
  pumpDevNewTokenLastAt: number;
  pumpDevNewTokenIsStalled: boolean;
  proxyPumpPortalLastNewTokenAt: Map<string, number>;
  seenMints: Set<string>;
  pumpPortalPingInterval?: NodeJS.Timeout;
  simultaneousPumpPortalStall: boolean;
  wasPumpPortalSimultaneouslyStalled: boolean;
  uniqueWallets: Set<string>;
  testStartAt: number;
  proxyReconnectPhases: Map<string, number>;
}

export const state: StressState = {
  isRunning: false,
  mode: null,
  sourceNewToken: true,
  testConnection: null,
  testSubscriptions: new Set(),
  testLastTradeAt: 0,
  testIsStalled: false,
  testLastResetAt: 0,
  proxies: new Map(),
  proxyWs: new Map(),
  totalTokens: 0,
  totalTrades: 0,
  totalRotations: 0,
  subscriptionsCount: 0,
  rotationCount: 0,
  simultaneousStall: false,
  reconnectStats: { best: Infinity, worst: 0, all: [] },
  tradeResumeStats: { best: Infinity, worst: 0, all: [] },
  logs: [],
  coordinatorLastNewTokenAt: 0,
  pumpDevNewTokenLastAt: 0,
  pumpDevNewTokenIsStalled: false,
  proxyPumpPortalLastNewTokenAt: new Map(),
  seenMints: new Set(),
  simultaneousPumpPortalStall: false,
  wasPumpPortalSimultaneouslyStalled: false,
  uniqueWallets: new Set(),
  testStartAt: 0,
  proxyReconnectPhases: new Map(),
};

export const PER_PROVIDER_LIMIT = 4950;
export const DETECTION_WINDOW = 5000;
export const CHECK_INTERVAL = 1000;
export const RESET_COOLDOWN = 30000;
export const PUMPPORTAL_STALL_THRESHOLD = 30000;

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
