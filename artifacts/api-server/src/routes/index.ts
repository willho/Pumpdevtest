import { Router, type IRouter } from "express";
import healthRouter from "./health";
import stressTestRouter from "./stress-test";

const router: IRouter = Router();

router.use(healthRouter);
router.use(stressTestRouter);

export default router;
