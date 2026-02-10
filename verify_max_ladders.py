import logging
import time
from unittest.mock import MagicMock

from config import StockStatus, StrategySettings
from dhan_client import DhanClientWrapper
from strategy_engine import LadderEngine

logging.basicConfig(level=logging.INFO)


def test_max_ladder_stocks_limits_new_starts():
    mock_dhan = MagicMock(spec=DhanClientWrapper)
    mock_dhan.is_connected = True
    counter = {"i": 0}

    def _place_order(**kwargs):
        counter["i"] += 1
        return {"status": "success", "orderId": str(counter["i"])}

    mock_dhan.place_order = MagicMock(side_effect=_place_order)

    engine = LadderEngine(mock_dhan)
    engine.running = True
    engine.is_market_hours = MagicMock(return_value=True)
    engine.update_settings(
        StrategySettings(
            max_ladder_stocks=20,
            top_n_gainers=10,
            top_n_losers=10,
            min_turnover_crores=1.0,
            trade_capital=5000.0,
        )
    )

    # Create 60 positive movers + 60 negative movers (all eligible by turnover/ltp)
    active = {}
    for i in range(60):
        sym = f"G{i:02d}"
        active[sym] = StockStatus(
            symbol=sym,
            mode="NONE",
            ltp=100.0,
            change_pct=5.0 - (i * 0.01),
            pnl=0.0,
            status="IDLE",
            entry_price=0.0,
            quantity=0,
            ladder_level=0,
            next_add_on=0.0,
            stop_loss=0.0,
            target=0.0,
            prev_close=95.0,
            day_open=95.0,
            open_gap_pct=0.0,
            turnover=2_00_00_000.0,  # 2 Cr
        )
    for i in range(60):
        sym = f"L{i:02d}"
        active[sym] = StockStatus(
            symbol=sym,
            mode="NONE",
            ltp=100.0,
            change_pct=-(5.0 - (i * 0.01)),
            pnl=0.0,
            status="IDLE",
            entry_price=0.0,
            quantity=0,
            ladder_level=0,
            next_add_on=0.0,
            stop_loss=0.0,
            target=0.0,
            prev_close=105.0,
            day_open=105.0,
            open_gap_pct=0.0,
            turnover=2_00_00_000.0,  # 2 Cr
        )

    engine.active_stocks = active

    # Run selection multiple times; should only start up to 10 longs + 10 shorts total.
    for _ in range(5):
        engine.select_top_movers()

    # Wait for async workers to execute queued orders
    deadline = time.time() + 5.0
    while time.time() < deadline:
        # Simulate Live Order Update websocket fills for any placed orders.
        try:
            pending_items = list(getattr(engine, "_pending_broker_actions", {}).items())
        except Exception:
            pending_items = []
        for oid, action in pending_items:
            engine.on_order_update(
                {
                    "Type": "order_alert",
                    "Data": {
                        "orderNo": str(oid),
                        "status": "TRADED",
                        "tradedQty": int(action.get("qty") or 0),
                        "avgTradedPrice": float(action.get("price") or 100.0),
                    },
                }
            )
        active_positions = [s for s in engine.active_stocks.values() if s.mode != "NONE"]
        if len(active_positions) == 20:
            break
        time.sleep(0.05)

    active_positions = [s for s in engine.active_stocks.values() if s.mode != "NONE"]
    assert len(active_positions) == 20, "Should start only 20 ladders total"
    assert len([s for s in active_positions if s.mode == "LONG"]) == 10
    assert len([s for s in active_positions if s.mode == "SHORT"]) == 10


def test_settings_enforce_sum_with_max_ladder_stocks():
    mock_dhan = MagicMock(spec=DhanClientWrapper)
    mock_dhan.is_connected = True
    engine = LadderEngine(mock_dhan)

    engine.update_settings(
        StrategySettings(
            max_ladder_stocks=5,
            top_n_gainers=3,
            top_n_losers=4,
        )
    )
    assert engine.settings.top_n_gainers == 3
    assert engine.settings.top_n_losers == 2, "Losers should be clamped so gainers+losers==max"


def test_session_max_blocks_new_symbols_even_if_capacity_frees_up():
    mock_dhan = MagicMock(spec=DhanClientWrapper)
    mock_dhan.is_connected = True

    engine = LadderEngine(mock_dhan)
    engine.running = True
    engine.update_settings(
        StrategySettings(
            max_ladder_stocks=5,
            top_n_gainers=3,
            top_n_losers=2,
            min_turnover_crores=1.0,
        )
    )

    # Simulate we've already started 5 unique symbols this session.
    engine.started_symbols = {"A", "B", "C", "D", "E"}

    engine.active_stocks = {
        "X1": StockStatus(
            symbol="X1",
            mode="NONE",
            ltp=100.0,
            change_pct=1.5,
            pnl=0.0,
            status="IDLE",
            entry_price=0.0,
            quantity=0,
            ladder_level=0,
            next_add_on=0.0,
            stop_loss=0.0,
            target=0.0,
            prev_close=98.0,
            turnover=2_00_00_000.0,
        )
    }

    engine.select_top_movers()
    assert engine.active_stocks["X1"].pending_order == ""
    assert engine.active_stocks["X1"].mode == "NONE"


if __name__ == "__main__":
    test_max_ladder_stocks_limits_new_starts()
    test_settings_enforce_sum_with_max_ladder_stocks()
    test_session_max_blocks_new_symbols_even_if_capacity_frees_up()
    print("OK")
