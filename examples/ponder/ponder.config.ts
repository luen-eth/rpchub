import { createConfig } from "ponder";
import { erc20Abi } from "viem";

const chainId = Number(process.env.PONDER_CHAIN_ID);
if (chainId !== 1 && chainId !== 56) {
  throw new Error("Set PONDER_CHAIN_ID to 1 (Ethereum) or 56 (BNB). Run a separate process for each chain.");
}
const startText = process.env.PONDER_START_BLOCK;
const startBlock = Number(startText);
if (!startText || !/^\d+$/.test(startText) || !Number.isSafeInteger(startBlock)) {
  throw new Error("Set PONDER_START_BLOCK to the first block you want to index.");
}
const rpcBase = process.env.RPCHUB_URL?.replace(/\/$/, "");
if (!rpcBase || !/^https?:\/\//.test(rpcBase)) {
  throw new Error("Set RPCHUB_URL to the HTTP(S) URL of your RPCHub service.");
}

export default createConfig({
  database: process.env.DATABASE_URL
    ? { kind: "postgres", connectionString: process.env.DATABASE_URL }
    : { kind: "pglite", directory: `./.ponder/pglite-${chainId}` },
  chains: { chain: { id: chainId, rpc: `${rpcBase}/${chainId}/indexer`, pollingInterval: 2000, ethGetLogsBlockRange: 10 } },
  blocks: { EveryBlock: { chain: "chain", startBlock, interval: 1 } },
  contracts: {
    Token: {
      abi: erc20Abi,
      chain: "chain",
      address: chainId === 1
        ? "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
        : "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
      startBlock,
    },
  },
});
