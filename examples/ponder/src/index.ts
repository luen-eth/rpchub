import { ponder } from "ponder:registry";
import { block, transfer } from "ponder:schema";

ponder.on("EveryBlock:block", async ({ event, context }) => {
  const row = {
    id: `${context.chain.id}:${event.block.number}`,
    chainId: context.chain.id,
    number: Number(event.block.number),
    hash: event.block.hash,
    parentHash: event.block.parentHash,
    timestamp: Number(event.block.timestamp),
  };
  await context.db.insert(block).values(row);
});

ponder.on("Token:Transfer", async ({ event, context }) => {
  await context.db.insert(transfer).values({
    id: `${context.chain.id}:${event.transaction.hash}:${event.log.logIndex}`,
    chainId: context.chain.id,
    number: Number(event.block.number),
    blockHash: event.block.hash,
    txHash: event.transaction.hash,
    logIndex: Number(event.log.logIndex),
  });
});
