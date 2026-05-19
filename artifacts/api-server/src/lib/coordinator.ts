import { WebSocketServer, WebSocket } from "ws";
import type { Server } from "http";
import { randomUUID } from "crypto";
import { db } from "@workspace/db";
import { tradesTable, migrationsTable } from "@workspace/db/schema";
import { state, log, MIGRATION_STALL_THRESHOLD } from "./stress-state.js";
import { checkTradeResumeCompletion } from "./stress-engine.js";
import { getMigrationStats } from "./migration-engine.js";

export function startCoordinator(server: Server) {
  const wss = new WebSocketServer({ noServer: true });

  const heartbeatInterval = setInterval(() => {
    for (const ws of wss.clients) {
      const wsAlive = (ws as WebSocket & { isAlive?: boolean });
      if (wsAlive.isAlive === false) {
        ws.terminate();
        continue;
      }
      wsAlive.isAlive = false;
      ws.ping();
    }
  }, 30000);

  wss.on("close", () => {
    clearInterval(heartbeatInterval);
  });

  server.on("upgrade", (req, socket, head) => {
    if (req.url?.startsWith("/coordinator")) {
      wss.handleUpgrade(req, socket as import("stream").Duplex, head, (ws) => {
        wss.emit("connection", ws, req);
      });
    } else {
      socket.destroy();
    }
  });

  wss.on("connection", (ws: WebSocket) => {
    let proxyId: string | null = null;
    (ws as WebSocket & { isAlive?: boolean }).isAlive = true;
    ws.on("pong", () => {
      (ws as WebSocket & { isAlive?: boolean }).isAlive = true;
    });

    ws.on("message", async (raw) => {
      try {
        const msg = JSON.parse(raw.toString()) as Record<string, unknown>;

        if (msg["type"] === "identify") {
          proxyId = randomUUID();
          const name = String(msg["name"] ?? "unnamed");
          const version = String(msg["version"] ?? "0.0.0");
          const capacity = Number(msg["capacity"] ?? 4950);

          const proxyIndex = state.proxies.size;
          const totalProxies = state.proxies.size + 1;
          const pumpportalPingOffsetMin = Math.floor(
            (proxyIndex * 30) / totalProxies
          );
          const migrationPingOffsetMin = Math.floor(
            (proxyIndex * 30) / totalProxies
          );
          state.proxies.set(proxyId, {
            id: proxyId,
            name,
            version,
            capacity,
            subscriptions: new Set(),
            lastTradeAt: Date.now(),
            connectedAt: Date.now(),
            isStalled: false,
            lastResetAt: 0,
            pumpPortalLastNewTokenAt: Date.now(),
            pumpPortalIsStalled: false,
          });
          state.proxyWs.set(proxyId, ws);
          state.proxyPumpPortalLastNewTokenAt.set(proxyId, Date.now());
          state.migrationProviderLastEventAt.set(name, Date.now());

          ws.send(
            JSON.stringify({
              type: "welcome",
              proxyId,
              pumpportalPingOffsetMin,
              migrationPingOffsetMin,
            })
          );
          log(
            `[coordinator] Proxy joined: ${name} v${version} capacity=${capacity} id=${proxyId.slice(0, 8)} pumpportal_offset=${pumpportalPingOffsetMin}min`
          );
          return;
        }

        if (!proxyId) return;
        const proxy = state.proxies.get(proxyId);
        if (!proxy) return;

        if (msg["type"] === "trade") {
          const mint = String(msg["mint"] ?? "");
          const signature = String(msg["signature"] ?? "");
          const receivedAt = Number(msg["receivedAt"] ?? Date.now());
          const wallet = msg["wallet"] ? String(msg["wallet"]) : null;

          if (!mint || !signature) return;

          const now = Date.now();
          proxy.lastTradeAt = now;
          proxy.isStalled = false;
          state.totalTrades++;

          if (wallet) state.uniqueWallets.add(wallet);

          await db.insert(tradesTable).values({
            mint,
            provider: proxyId,
            signature,
            wallet,
            receivedAt,
          });

          await checkTradeResumeCompletion(proxyId, now);
        }

        if (msg["type"] === "discovered_token") {
          const mint = String(msg["mint"] ?? "");
          if (!mint) return;

          const now = Date.now();
          proxy.pumpPortalLastNewTokenAt = now;
          proxy.pumpPortalIsStalled = false;
          state.proxyPumpPortalLastNewTokenAt.set(proxyId, now);

          if (state.seenMints.has(mint)) return;

          state.seenMints.add(mint);
          log(
            `[coordinator] New token (proxy): ${mint.slice(0, 8)}... from ${proxy.name}`
          );
        }

        if (msg["type"] === "migration_detected") {
          const mint = String(msg["mint"] ?? "");
          const poolAddress = String(msg["poolAddress"] ?? "");
          const signature = String(msg["signature"] ?? "");
          const provider = String(msg["provider"] ?? "");
          if (!mint || !poolAddress || !signature) return;

          const now = Date.now();
          state.migrationProviderLastEventAt.set(provider, now);
          state.migrationProvidersStalled.delete(provider);
          state.totalMigrations++;

          if (!state.seenMints.has(mint)) {
            state.seenMints.add(mint);
            log(
              `[coordinator] Migration detected: ${mint.slice(0, 8)}... (pool: ${poolAddress.slice(0, 8)}...)`
            );
          }

          await db.insert(migrationsTable).values({
            mint,
            poolAddress,
            signature,
            provider,
            detectedAt: now,
            mintAmount: msg["mintAmount"] ? String(msg["mintAmount"]) : "0",
            solAmount: msg["solAmount"] ? String(msg["solAmount"]) : "0",
          });
        }
      } catch (e: unknown) {
        log(`[coordinator] Message error: ${(e as Error).message}`, "error");
      }
    });

    ws.on("close", () => {
      if (proxyId) {
        const proxy = state.proxies.get(proxyId);
        log(
          `[coordinator] Proxy disconnected: ${proxy?.name ?? proxyId.slice(0, 8)}`,
          "warn"
        );
        state.proxies.delete(proxyId);
        state.proxyWs.delete(proxyId);
      }
    });

    ws.on("error", (err) => {
      log(`[coordinator] Proxy WS error: ${err.message}`, "error");
      if (proxyId) {
        state.proxies.delete(proxyId);
        state.proxyWs.delete(proxyId);
      }
    });
  });

  log("[coordinator] Listening on /coordinator");
}

export function startDiscoveryStreams(): void {
  for (const [proxyId, ws] of state.proxyWs) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: "start_discovery" }));
    }
  }
  log("[coordinator] Sent start_discovery signal to all proxies");
}

export function checkMigrationProviderStall(): void {
  const now = Date.now();

  for (const [provider, lastEventTime] of state.migrationProviderLastEventAt) {
    const timeSilent = now - lastEventTime;

    if (timeSilent > MIGRATION_STALL_THRESHOLD) {
      if (!state.migrationProvidersStalled.has(provider)) {
        state.migrationProvidersStalled.add(provider);
        log(
          `[stall-detector] Migration provider ${provider} stalled (${timeSilent}ms), needs reset`,
          "warn"
        );
      }
    } else {
      state.migrationProvidersStalled.delete(provider);
    }
  }
}
