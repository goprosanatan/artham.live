# ===================================================================================================
# ORDER - HELPER
# ===================================================================================================



# ===================================================================================================
# ORDERS


def _decode(value):
	return value.decode() if isinstance(value, (bytes, bytearray)) else value

async def enqueue_command(redis_conn, stream: str, data: dict, maxlen: int = 100000):
	"""Push a structured OMS command to the Redis stream."""
	return await redis_conn.xadd(
		stream,
		{_decode(k): _decode(v) for k, v in data.items()},
		maxlen=maxlen,
		approximate=True,
	)


async def list_brackets(redis_conn, namespace: str, limit: int = 100):
	"""Return up to `limit` bracket orders stored in Redis hash keys under the given namespace.
	Skips soft-deleted brackets (marked with deleted=1).
	"""
	orders = []
	pattern = f"{namespace}:*"
	
	# Collect bracket keys in batches
	bracket_keys = []
	async for key in redis_conn.scan_iter(match=pattern, count=200):
		bracket_keys.append(key)
		if len(bracket_keys) >= limit:
			break
	
	if not bracket_keys:
		return orders
	
	# Batch fetch all brackets using pipeline for speed
	pipe = redis_conn.pipeline()
	for key in bracket_keys:
		pipe.hgetall(key)
	
	results = await pipe.execute()
	
	for data in results:
		if data:
			decoded = { _decode(k): _decode(v) for k, v in data.items() }
			# Skip soft-deleted brackets
			if decoded.get("deleted") in ["1", 1, "true", True]:
				continue
			# Normalize symbol/trading_symbol naming for frontend convenience
			if decoded.get("symbol") and not decoded.get("trading_symbol"):
				decoded["trading_symbol"] = decoded.get("symbol")
			orders.append(decoded)
	
	# Sort by created_at descending (newest first)
	try:
		orders.sort(key=lambda x: x.get("created_at", ""), reverse=True)
	except Exception:
		pass
	
	return orders


async def list_pending_intents(redis_conn, stream: str, limit: int = 50):
	"""Return up to `limit` most recent queued commands from the OMS command stream.
	Skips intents that match existing brackets (including soft-deleted ones) and intents
	for explicitly deleted brackets.
	"""
	orders = []
	entries = await redis_conn.xrevrange(stream, count=limit)
	
	# Fetch ALL brackets (including deleted) for deduplication purposes
	all_brackets = []
	pattern = "oms:bracket:*"
	pipe = redis_conn.pipeline()
	bracket_keys = []
	async for key in redis_conn.scan_iter(match=pattern, count=100):
		bracket_keys.append(key)
	
	for key in bracket_keys:
		pipe.hgetall(key)
	
	if pipe.command_stack:
		bracket_results = await pipe.execute()
		for bracket_data in bracket_results:
			if bracket_data:
				bracket_decoded = { _decode(k): _decode(v) for k, v in bracket_data.items() }
				all_brackets.append(bracket_decoded)
	
	for msg_id, values in entries:
		payload = { _decode(k): _decode(v) for k, v in values.items() }
		command = (payload.get("command") or "").upper()
		
		# Only include PLACE_BRACKET intents
		if command != "PLACE_BRACKET":
			continue
		
		# If this intent has a bracket_id, check if it exists (skip if it does)
		bracket_id = payload.get("bracket_id")
		if bracket_id:
			bracket_exists = any(b.get("bracket_id") == bracket_id for b in all_brackets)
			if bracket_exists:
				continue  # This command already created a bracket
		
		# Check if this intent matches any existing bracket (has already been persisted)
		# Compare against ALL brackets to catch deleted orders too
		intent_matches_bracket = False
		for bracket in all_brackets:
			if (str(payload.get("instrument_id")) == str(bracket.get("instrument_id"))
				and (payload.get("side") or "").upper() == (bracket.get("side") or "").upper()
				and int(payload.get("qty", 0)) == int(bracket.get("qty", 0))
				and str(payload.get("entry_price")) == str(bracket.get("entry_price"))
				and str(payload.get("target_price")) == str(bracket.get("target_price"))
				and str(payload.get("stoploss_price")) == str(bracket.get("stoploss_price"))):
				intent_matches_bracket = True
				break
		
		if intent_matches_bracket:
			continue  # Skip intents already persisted (even if deleted)
		
		payload["state"] = "QUEUED"
		payload["stream_id"] = _decode(msg_id)
		if payload.get("symbol") and not payload.get("trading_symbol"):
			payload["trading_symbol"] = payload.get("symbol")
		orders.append(payload)
	
	return orders


async def get_bracket(redis_conn, bracket_id: str):
	"""Fetch a single bracket by ID."""
	data = await redis_conn.hgetall(f"oms:bracket:{bracket_id}")
	if not data:
		return None
	
	decoded = { _decode(k): _decode(v) for k, v in data.items() }
	# Return None if bracket is soft-deleted
	if decoded.get("deleted") in ["1", 1, "true", True]:
		return None
	if decoded.get("symbol") and not decoded.get("trading_symbol"):
		decoded["trading_symbol"] = decoded.get("symbol")
	
	return decoded


async def soft_delete_bracket(redis_conn, bracket_id: str):
	"""Soft-delete a bracket by marking it as deleted instead of removing it.
	This prevents ghost orders while preserving audit trail.
	
	Returns: tuple (success: bool, message: str)
	"""
	try:
		bracket = await redis_conn.hgetall(f"oms:bracket:{bracket_id}")
		if not bracket:
			return False, f"Bracket {bracket_id} not found"
		
		bracket_decoded = { _decode(k): _decode(v) for k, v in bracket.items() }
		if bracket_decoded.get("deleted") in ["1", 1, "true", True]:
			return False, f"Bracket {bracket_id} already deleted"
		
		# Soft delete: mark as deleted instead of removing
		await redis_conn.hset(f"oms:bracket:{bracket_id}", "deleted", "1")
		
		# Remove from active sets to stop tracking as active
		instrument_id = bracket_decoded.get("instrument_id")
		strategy_id = bracket_decoded.get("strategy_id")
		
		if instrument_id:
			await redis_conn.srem(f"oms:active:instrument:{instrument_id}", bracket_id)
		if strategy_id:
			await redis_conn.srem(f"oms:active:strategy:{strategy_id}", bracket_id)
		await redis_conn.srem("oms:active:brackets", bracket_id)
		
		return True, f"Bracket {bracket_id} deleted"
		
	except Exception as e:
		return False, f"Error deleting bracket: {str(e)}"
