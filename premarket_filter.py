"""
Premarket Filtration Script
============================
Run this script before market hours to filter stocks by volume SMA criteria.
The filtered candidates are saved to 'filtered_stocks.json' for the strategy engine to use.

Usage:
    python premarket_filter.py
"""

import asyncio
import json
import logging
import argparse
import hashlib
from datetime import datetime
from typing import Dict, Tuple, Optional, Iterable, List
from dhan_client import DhanClientWrapper
from credentials_store import load_credentials
from redis_store import save_candidates, load_candidates
from strategy_engine import STOCK_LIST

# Setup logging (IST timestamps)
from zoneinfo import ZoneInfo
IST = ZoneInfo("Asia/Kolkata")

class ISTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, IST)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat(sep=" ", timespec="milliseconds")

handler = logging.StreamHandler()
handler.setFormatter(ISTFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.handlers = [handler]
logger = logging.getLogger(__name__)

# Filtration criteria
VOLUME_SMA_THRESHOLD = 50
VOLUME_SMA_DIVISOR = 1875
REQUIRED_DAYS = 5


def _stock_list_signature(symbols: Iterable[str]) -> Tuple[int, str]:
    """
    Compute a stable signature for a stock list.

    - Normalizes symbols (strip/upper)
    - De-duplicates
    - Hash is order-independent (sorted)
    """
    normalized = [str(s).strip().upper() for s in symbols if str(s).strip()]
    unique_sorted = sorted(set(normalized))
    payload = "\n".join(unique_sorted).encode("utf-8")
    return len(unique_sorted), hashlib.sha256(payload).hexdigest()

def _normalize_symbol(symbol: str) -> str:
    return str(symbol).strip().upper()


class PremarketFilter:
    """Handles premarket stock filtration based on volume SMA."""
    
    def __init__(self, dhan_client: Optional[DhanClientWrapper] = None, verbose: bool = False):
        self.dhan_client = dhan_client
        self.verbose = verbose
        self._last_total_screened: Optional[int] = None
        self._last_stock_list_count: Optional[int] = None
        self._last_stock_list_hash: Optional[str] = None
        self._last_volume_sma_by_symbol: Dict[str, float] = {}
        
    async def filter_single_stock(self, symbol: str) -> Optional[Tuple[str, float, float]]:
        """
        Filter a single stock based on volume SMA criteria.
        
        Args:
            symbol: Stock symbol to filter
            
        Returns:
            Tuple of (symbol, prev_close, volume_sma_1m) if accepted, None if rejected
        """
        try:
            if self.dhan_client is None:
                raise RuntimeError("Dhan client not initialized (cannot filter stocks).")

            # Fetch historical data
            df = await self.dhan_client.get_historical_data_async(symbol, days=15)
            
            if df is None or df.empty or len(df) < REQUIRED_DAYS:
                logger.debug(f"REJECTED {symbol}: Insufficient data")
                return None
                
            # Get last 5 days
            last_5_days = df.tail(REQUIRED_DAYS)
            
            if 'volume' not in last_5_days.columns:
                logger.debug(f"REJECTED {symbol}: No volume data")
                return None
                
            # Calculate volume SMA
            total_volume_5d = last_5_days['volume'].sum()
            volume_sma = total_volume_5d / VOLUME_SMA_DIVISOR
            
            # Apply filter
            if volume_sma > VOLUME_SMA_THRESHOLD:
                prev_close = float(df.iloc[-1]['close'])
                if self.verbose:
                    logger.info(
                        f"ACCEPTED {symbol}: VolSMA={volume_sma:.2f}, PrevClose={prev_close:.2f}"
                    )
                return (symbol, prev_close, float(volume_sma))
            else:
                logger.debug(f"REJECTED {symbol}: VolSMA={volume_sma:.2f} (threshold: {VOLUME_SMA_THRESHOLD})")
                return None
                
        except Exception as e:
            logger.error(f"ERROR filtering {symbol}: {e}")
            return None
    
    async def filter_all_stocks(
        self,
        symbols: Optional[Iterable[str]] = None,
        max_in_flight: int = 20,
    ) -> Dict[str, float]:
        """
        Filter all stocks using concurrent processing.
        
        Args:
            symbols: Iterable of symbols to process (defaults to STOCK_LIST)
            max_in_flight: Max concurrent in-flight tasks (threadpool-bound)
        
        Returns:
            Dictionary mapping accepted symbols to their previous close prices
        """
        raw_symbols: List[str] = list(symbols) if symbols is not None else list(STOCK_LIST)
        symbols_list: List[str] = []
        seen = set()
        for sym in raw_symbols:
            norm = _normalize_symbol(sym)
            if not norm or norm in seen:
                continue
            seen.add(norm)
            symbols_list.append(norm)
        total_stocks = len(symbols_list)
        sig_count, sig_hash = _stock_list_signature(symbols_list)
        self._last_total_screened = total_stocks
        self._last_stock_list_count = sig_count
        self._last_stock_list_hash = sig_hash

        logger.info(f"Starting Volume SMA Filtration on {total_stocks} stocks...")
        logger.info(f"Criteria: Volume SMA > {VOLUME_SMA_THRESHOLD}")
        logger.info(f"Concurrency: max_in_flight={max_in_flight}")
        logger.info("=" * 70)
        
        accepted_stocks: Dict[str, float] = {}
        accepted_volume_sma: Dict[str, float] = {}

        semaphore = asyncio.Semaphore(max(1, int(max_in_flight)))
        completed = 0

        async def _run_one(sym: str):
            async with semaphore:
                return await self.filter_single_stock(sym)

        tasks = [asyncio.create_task(_run_one(sym)) for sym in symbols_list]
        for fut in asyncio.as_completed(tasks):
            result = await fut
            completed += 1

            if result and isinstance(result, tuple):
                sym, prev_close, volume_sma = result
                accepted_stocks[sym] = prev_close
                try:
                    accepted_volume_sma[sym] = float(volume_sma)
                except Exception:
                    pass

            if completed % 25 == 0 or completed == total_stocks:
                progress = (completed / total_stocks) * 100
                logger.info(
                    f"[{completed}/{total_stocks}] ({progress:.1f}%) "
                    f"Accepted={len(accepted_stocks)}"
                )
        
        logger.info("=" * 70)
        logger.info(f"Filtration Complete: {len(accepted_stocks)} / {total_stocks} stocks accepted")
        self._last_volume_sma_by_symbol = dict(accepted_volume_sma)
        return accepted_stocks
    
    def save_to_json(
        self,
        candidates: Dict[str, float],
        filepath: str = "filtered_stocks.json",
        *,
        metadata: Optional[Dict] = None,
        save_redis: bool = True,
    ):
        """
        Save filtered candidates to JSON file.
        
        Args:
            candidates: Dictionary of symbol -> prev_close
            filepath: Path to save JSON file
        """
        data: Dict = dict(metadata) if metadata else {}
        data.setdefault("timestamp", datetime.now().isoformat())
        data.setdefault(
            "criteria",
            {
                "volume_sma_threshold": VOLUME_SMA_THRESHOLD,
                "volume_sma_divisor": VOLUME_SMA_DIVISOR,
                "required_days": REQUIRED_DAYS,
            },
        )
        data["candidates"] = candidates
        volume_sma_by_symbol = None
        if metadata and isinstance(metadata.get("volume_sma_by_symbol"), dict):
            volume_sma_by_symbol = metadata.get("volume_sma_by_symbol")
        elif getattr(self, "_last_volume_sma_by_symbol", None):
            volume_sma_by_symbol = self._last_volume_sma_by_symbol
        if volume_sma_by_symbol:
            data["volume_sma_by_symbol"] = volume_sma_by_symbol
        data["stocks_accepted"] = len(candidates)

        if "total_stocks_screened" not in data:
            data["total_stocks_screened"] = (
                int(self._last_total_screened)
                if self._last_total_screened is not None
                else len(STOCK_LIST)
            )

        # Stock-list signature helps prevent reusing cache from a different symbol universe.
        if "stock_list_count" not in data or "stock_list_hash" not in data:
            if self._last_stock_list_count is not None and self._last_stock_list_hash is not None:
                data.setdefault("stock_list_count", int(self._last_stock_list_count))
                data.setdefault("stock_list_hash", str(self._last_stock_list_hash))
            else:
                count, digest = _stock_list_signature(STOCK_LIST)
                data.setdefault("stock_list_count", count)
                data.setdefault("stock_list_hash", digest)
        else:
            # Ensure types are JSON-serializable (in case Redis returned non-primitive types)
            try:
                data["stock_list_count"] = int(data.get("stock_list_count"))
                data["stock_list_hash"] = str(data.get("stock_list_hash"))
            except Exception:
                count, digest = _stock_list_signature(STOCK_LIST)
                data["stock_list_count"] = count
                data["stock_list_hash"] = digest
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Saved {len(candidates)} candidates to {filepath}")
        
        # Also store in Redis for all-day reuse
        if save_redis:
            if save_candidates(candidates, data):
                logger.info("Saved candidates to Redis (valid for the day)")
            else:
                logger.warning("Could not save candidates to Redis")


async def main():
    """Main execution function."""
    logger.info("=" * 70)
    logger.info("PREMARKET STOCK FILTRATION - Volume SMA Filter")
    logger.info("=" * 70)

    parser = argparse.ArgumentParser(description="Premarket stock filtration (volume SMA).")
    parser.add_argument("--rate", type=float, default=3.0, help="Historical-data requests per second (default: 3)")
    parser.add_argument("--connections", type=int, default=5, help="Max concurrent HTTP connections (default: 5)")
    parser.add_argument("--in-flight", type=int, default=5, help="Max in-flight symbol tasks (default: 5)")
    parser.add_argument("--force", action="store_true", help="Force recompute even if Redis has today's candidates")
    parser.add_argument("--verbose", action="store_true", help="Log every accepted stock")
    args = parser.parse_args()

    if args.rate <= 0:
        parser.error("--rate must be > 0")
    if args.connections <= 0:
        parser.error("--connections must be > 0")
    if args.in_flight <= 0:
        parser.error("--in-flight must be > 0")
    
    # Load credentials
    import os
    from getpass import getpass
    
    # If we already have today's candidates in Redis, reuse by default.
    if not args.force:
        cached = load_candidates()
        if cached and cached.get("candidates"):
            # Guardrail: never allow cached candidates outside current STOCK_LIST.
            stock_set = {_normalize_symbol(s) for s in STOCK_LIST}

            current_count, current_hash = _stock_list_signature(STOCK_LIST)
            cached_count = cached.get("stock_list_count")
            cached_hash = cached.get("stock_list_hash")

            # If the cached results were computed for a different symbol universe, don't reuse them.
            if cached_count is not None and cached_hash is not None:
                try:
                    cached_count = int(cached_count)
                    cached_hash = str(cached_hash)
                except Exception:
                    cached_count, cached_hash = None, None

            if cached_count is not None and cached_hash is not None:
                if cached_count != current_count or cached_hash != current_hash:
                    logger.info(
                        "Redis cache found, but STOCK_LIST changed "
                        f"(cached_count={cached_count}, current_count={current_count}). Recomputing..."
                    )
                else:
                    raw_candidates = cached.get("candidates", {}) or {}
                    candidates = {
                        _normalize_symbol(sym): float(prev_close)
                        for sym, prev_close in raw_candidates.items()
                        if _normalize_symbol(sym) in stock_set
                    }
                    dropped = len(raw_candidates) - len(candidates)
                    if dropped:
                        logger.warning(
                            f"Redis cache contained {dropped} symbols not in current STOCK_LIST; ignoring them."
                        )
                    ts = cached.get("timestamp", "unknown")
                    screened = cached.get("total_stocks_screened", "unknown")
                    logger.info(
                        "Using cached candidates from Redis "
                        f"(timestamp={ts}, screened={screened}, accepted={len(candidates)}). "
                        f"Use --force to rescreen {len(STOCK_LIST)} stocks."
                    )
                    pf = PremarketFilter(verbose=args.verbose)
                    pf.save_to_json(candidates, metadata=cached, save_redis=False)
                    return
            else:
                # Legacy cache (no stock list signature). Only reuse if it's fully compatible.
                raw_candidates = cached.get("candidates", {}) or {}
                cached_syms = {_normalize_symbol(sym) for sym in raw_candidates.keys()}
                outside = cached_syms - stock_set
                if outside:
                    sample = ", ".join(sorted(list(outside))[:8])
                    logger.warning(
                        "Legacy Redis cache contains symbols not present in current STOCK_LIST "
                        f"(outside_count={len(outside)}; e.g. {sample}). Recomputing..."
                    )
                else:
                    candidates = {
                        _normalize_symbol(sym): float(prev_close)
                        for sym, prev_close in raw_candidates.items()
                    }
                    ts = cached.get("timestamp", "unknown")
                    screened = cached.get("total_stocks_screened", "unknown")
                    logger.info(
                        "Using legacy cached candidates from Redis "
                        f"(timestamp={ts}, screened={screened}, accepted={len(candidates)}). "
                        "Upgrading cache with stock-list signature. "
                        f"Use --force to rescreen {len(STOCK_LIST)} stocks."
                    )
                    upgraded_meta = dict(cached)
                    upgraded_meta["stock_list_count"] = current_count
                    upgraded_meta["stock_list_hash"] = current_hash
                    pf = PremarketFilter(verbose=args.verbose)
                    pf.save_to_json(candidates, metadata=upgraded_meta, save_redis=True)
                    return

    # Only require credentials if we're going to recompute.
    # Try environment variables first
    client_id = os.getenv("DHAN_CLIENT_ID")
    access_token = os.getenv("DHAN_ACCESS_TOKEN")

    # If not in env, try saved credentials file
    if not client_id or not access_token:
        saved_client_id, saved_access_token = load_credentials()
        client_id = client_id or saved_client_id
        access_token = access_token or saved_access_token

    # If still missing, prompt user
    if not client_id or not access_token:
        logger.info("\nDhan API Credentials Required")
        logger.info("You can set DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN environment variables")
        logger.info("or save credentials once from the web app and re-run:")
        logger.info("  - Click 'Save Settings' after entering credentials")
        logger.info("or enter them now:\n")

        if not client_id:
            client_id = input("Enter Dhan Client ID: ").strip()
        if not access_token:
            access_token = getpass("Enter Dhan Access Token: ").strip()

    if not client_id or not access_token:
        logger.error("Credentials are required. Exiting.")
        return

    # Initialize Dhan client (tuned for historical API rate limits)
    logger.info("Initializing Dhan client...")
    dhan_client = DhanClientWrapper(
        max_requests_per_second=float(args.rate),
        max_connections=int(args.connections),
    )
    
    # Skip slow security master prefetch (not needed for historical data filtering)
    success, message = dhan_client.connect(client_id, access_token, False)
    if not success:
        logger.error(f"Failed to connect to Dhan API: {message}")
        logger.error("Please check your credentials and try again.")
        return
    
    logger.info("Dhan client connected successfully")

    # Ensure security master mapping is available before screening symbols.
    logger.info("Loading security master mapping...")
    if not dhan_client.ensure_security_mapping_loaded():
        logger.error("Failed to load security master mapping. Cannot run filtration.")
        return
    
    # Run filtration
    filter_engine = PremarketFilter(dhan_client, verbose=args.verbose)
    candidates = await filter_engine.filter_all_stocks(max_in_flight=int(args.in_flight))
    
    # Save results
    filter_engine.save_to_json(candidates)
    
    # Print summary
    logger.info("=" * 70)
    logger.info("SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Total stocks screened: {len(STOCK_LIST)}")
    logger.info(f"Stocks accepted: {len(candidates)}")
    logger.info(f"Acceptance rate: {len(candidates)/len(STOCK_LIST)*100:.1f}%")
    
    if candidates:
        logger.info("\nAccepted Stocks:")
        for symbol, prev_close in sorted(candidates.items()):
            logger.info(f"  {symbol}: ₹{prev_close:.2f}")
    
    logger.info("=" * 70)
    logger.info("Filtration complete! You can now start the strategy engine.")


if __name__ == "__main__":
    asyncio.run(main())
