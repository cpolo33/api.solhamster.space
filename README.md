# Serum History

Collects and aggregates trades from serum dex for display in a tradingview chart.
This is powering the charts on [HAMS Dex](https://dex.solhamster.space/).
Feel free to improve and extend for the benefit for the larger solana ecosystem.



## Configuration

* Markets: should be added to the dictionaries in src/index.ts
  marketsV3 - for wrapped token denominated markets (deprecated)
  nativeMarketsV3 - for native token denominated markets

* All other configuration should be handled via environment variables.
  So far the following variables exist:

```
REDISCLOUD_URL: redis connection url
REDIS_MAX_CONN: maximum number of concurrent connections used by the redis pool
RPC_ENDPOINT_URL: solana rpc connection url
INTERVAL: time in seconds to wait between event queue polls
```
