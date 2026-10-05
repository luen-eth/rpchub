import { db } from "ponder:api";
import { block } from "ponder:schema";
import { Hono } from "hono";
import { sql } from "ponder";

const app = new Hono();
app.get("/progress", async (c) => {
  const [row] = await db.select({
    blocks: sql<number>`count(*)::int`,
    latestBlock: sql<number>`max(${block.number})`,
    latestTimestamp: sql<number>`max(${block.timestamp})`,
  }).from(block);
  const lagSeconds = row?.latestTimestamp == null ? null : Math.max(0, Date.now() / 1000 - row.latestTimestamp);
  // This measures indexed chain time, unlike /ready which only reports readiness.
  const threshold = Number(process.env.PONDER_CHAIN_ID) === 1 ? 60 : 30;
  const healthy = lagSeconds !== null && lagSeconds <= threshold;
  return c.json({ ...row, lagSeconds, healthy }, healthy ? 200 : 503);
});
export default app;
