import { Router, type IRouter } from "express";
import healthRouter from "./health";
import stressTestRouter from "./stress-test";
import migrationsRouter from "./migrations";

const router: IRouter = Router();

router.use(healthRouter);
router.use(stressTestRouter);
router.use(migrationsRouter);

export default router;
