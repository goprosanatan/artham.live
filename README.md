# artham.live

Real-time market data processing platform with event-streaming architecture for live tick ingestion, analytics, and trading workflows.

## 🏗 Architecture Overview

```
Exchange Feed (Zerodha/Kite)
        │
        ▼
┌─────────────────────────┐
│  tick_01_ingestor       │  ← WebSocket connection to Kite API
└─────────────────────────┘
        │ publishes to
        ▼
Redis Stream: md:ticks
        │
        ├─────────────────┬─────────────────┐
        ▼                 ▼                 ▼
┌───────────────┐  ┌───────────────┐   ┌───────────────────┐
│ 02_tick_store │  │ 02_bar_builder│   │ 02_feature_engine │
│ (PostgreSQL)  │  │ (OHLC bars)   │   │ (Greeks, deltas)  │
└───────────────┘  └───────────────┘   └───────────────────┘
        ▲                  │                    │
        │                  ▼                    │
        │         (md:bars:live.{tf}            ▼     
        │         and md:bars:final.{tf})      md:features
        │                  │                    │
        │                  │                    │
        │      ┌──────────────────────┐         │
        │      │    03_bar_store      │         │
        │      │  (Final Bars to DB)  │         │
        │      │                      │         │
        │      └──────────────────────┘         │
        │                  │                    │
        └──────────────────┴────────────────────┘
                           │
                           ▼
                ┌─────────────────────┐
                │   user_api          │  ← FastAPI + WebSocket
                │   (Consumer Group)  │     (Fanout to Clients)
                └─────────────────────┘
                           │
                    ┌──────┴──────┐
                    ▼             ▼
            ┌────────────┐  ┌────────────┐
            │ WebSocket  │  │ REST API   │
            │  Clients   │  │ Clients    │
            └────────────┘  └────────────┘
                    │             │
                    └──────┬──────┘
                           ▼
                ┌─────────────────────┐
                │   user_web          │  ← React UI
                └─────────────────────┘
```

## 📄 License

Proprietary - All rights reserved

## 👤 Author

**Shubham Sanatan**  
Repository: artham.live  
Branch: develop_beta


<!-- 
Tables represent what the data is.
Rows represent who the data belongs to.
 -->


 <!-- 
 SELECT create_hypertable(
  'bars_1m',
  'bar_start_ts',
  partitioning_column => 'instrument_id',
  number_partitions => 8
);
 
 
  -->


  <!-- Quant rule:
Market logic must live on the backend, not in browsers.

Ticks used for storage and aggregation must be centralized and authoritative.

A live candle in bar_builder = A continuously updated snapshot of the current bar state, built from authoritative ticks

 -->



 <!-- 

 PROFESSIONAL QUANT SHOP PRINCIPLES
 
 Market truth is centralized
Derived data is deterministic and rebuildable
Live systems and historical systems never mix concerns

 -->


 <!-- QUANT GOLDEN RULE
 
 If you can’t replay it, you don’t understand it.
 
  -->