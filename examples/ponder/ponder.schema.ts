import { onchainTable } from "ponder";

export const block = onchainTable("block", (p) => ({
  id: p.text().primaryKey(),
  chainId: p.integer().notNull(),
  number: p.integer().notNull(),
  hash: p.text().notNull(),
  parentHash: p.text().notNull(),
  timestamp: p.integer().notNull(),
}));

export const transfer = onchainTable("transfer", (p) => ({
  id: p.text().primaryKey(),
  chainId: p.integer().notNull(),
  number: p.integer().notNull(),
  blockHash: p.text().notNull(),
  txHash: p.text().notNull(),
  logIndex: p.integer().notNull(),
}));
