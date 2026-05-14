import { WebSocketServer, WebSocket } from "ws";
import type { Server } from "http";
import { randomUUID } from "crypto";
import { db } from "@workspace/db";
import { tradesTable } from "@workspace/db/schema";
import { state, log } from "./stress-state.js";
import { checkTradeResumeCompletion } from "./stress-engine.js";

export function startCoordinator(server: Server) {
  const wss = new WebSocketServer({ noServer: true });

  server.on("upgrade", (req, socket, head) => {
    if (req.url === "/coordinator") {
      wss.handleUpgrade(req, socket as import("stream").Duplex, head, (ws) => {
        wss.emit("connection", ws, req);
      });
    } else {
      socket.destroy();
    }
  });

  wss.on("connection", (ws: WebSocket) => {
    let proxyId: string | null = null;

    ws.on("message", async (raw) => {
      try {
        const msg = JSON.parse(raw.toString()) as Record<string, unknown>;

        if (msg["type"] === "identify") {
          proxyId = randomUUID();
          const name = String(msg["name"] ?? "unnamed");
          const version = String(msg["version"] ?? "0.0.0");
          const capacity = Number(msg["capacity"] ?? 4950);

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
          });
          state.proxyWs.set(proxyId, ws);

          ws.send(JSON.stringify({ type: "welcome", proxyId }));
          log(
            `[coordinator] Proxy joined: ${name} v${version} capacity=${capacity} id=${proxyId.slice(0, 8)}`
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

          if (!mint || !signature) return;

          const now = Date.now();
          proxy.lastTradeAt = now;
          proxy.isStalled = false;
          state.totalTrades++;

          await db.insert(tradesTable).values({
            mint,
            provider: proxyId,
            signature,
            receivedAt,
          });

          await checkTradeResumeCompletion(proxyId, now);
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
