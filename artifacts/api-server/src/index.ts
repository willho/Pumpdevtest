import http from "http";
import app from "./app.js";
import { logger } from "./lib/logger.js";
import { startCoordinator } from "./lib/coordinator.js";
import { startTest } from "./lib/stress-engine.js";

const rawPort = process.env["PORT"];

if (!rawPort) {
  throw new Error("PORT environment variable is required but was not provided.");
}

const port = Number(rawPort);

if (Number.isNaN(port) || port <= 0) {
  throw new Error(`Invalid PORT value: "${rawPort}"`);
}

const server = http.createServer(app);

startCoordinator(server);

server.listen(port, (err?: Error) => {
  if (err) {
    logger.error({ err }, "Error listening on port");
    process.exit(1);
  }
  logger.info({ port }, "Server listening");
  startTest(true, false).catch((e: Error) =>
    logger.error({ err: e }, "Auto-start failed")
  );
});
