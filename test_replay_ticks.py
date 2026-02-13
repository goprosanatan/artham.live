#!/usr/bin/env python3
"""
Quick test to verify replay_01_engine loads and publishes full ticks.
Run this after starting a replay session.
"""

import asyncio
import json
from redis.asyncio import Redis
from decouple import config

async def test_replay_ticks():
    redis_conn = Redis(
        host=config("REDIS_HOST", cast=str),
        port=config("REDIS_PORT", cast=int),
        decode_responses=True,
    )
    
    # Use a recent session ID or set one manually
    session_id = input("Enter session_id to inspect (or leave empty to list sessions): ").strip()
    
    if not session_id:
        # List all replay session streams
        keys = await redis_conn.keys("replay:*:md:ticks")
        print(f"\n=== Found {len(keys)} replay tick streams ===")
        for key in keys[:5]:  # Show first 5
            print(f"  {key}")
        if not keys:
            print("No replay sessions found!")
            return
        if len(keys) == 1:
            session_id = keys[0].split(":")[1]
            print(f"\nUsing session: {session_id}")
        else:
            return
    
    ticks_stream = f"replay:{session_id}:md:ticks"
    
    # Get first few messages from stream
    messages = await redis_conn.xread(
        streams={ticks_stream: "0"},
        count=3,
    )
    
    if not messages:
        print(f"No ticks found in {ticks_stream}")
        return
    
    print(f"\n=== First 3 ticks from {ticks_stream} ===\n")
    
    for _, entries in messages:
        for idx, (msg_id, tick_data) in enumerate(entries, 1):
            print(f"--- Tick {idx} (ID: {msg_id}) ---")
            # Show most important fields
            important_fields = [
                "instrument_id", "instrument_type", "exchange_ts", "ingest_ts",
                "last_price", "volume_traded", "oi", "tradable",
                "ohlc_open", "ohlc_high", "ohlc_low", "ohlc_close",
                "depth_buy_0_price", "depth_sell_0_price",
                "session_id", "source"
            ]
            
            for field in important_fields:
                if field in tick_data:
                    value = tick_data[field]
                    print(f"  {field:25s} = {value}")
            
            # Count total fields
            total_fields = len(tick_data)
            print(f"\n  Total fields: {total_fields}")
            print()
    
    await redis_conn.close()

if __name__ == "__main__":
    asyncio.run(test_replay_ticks())
