import time
from unittest.mock import MagicMock

from config import StockStatus, StrategySettings
from dhan_client import DhanClientWrapper
from strategy_engine import LadderEngine


def test_start_order_is_async_and_tick_not_blocked():
    mock_dhan = MagicMock(spec=DhanClientWrapper)
    mock_dhan.is_connected = True
    order_update_cb = {"cb": None}

    def _set_order_update_callback(cb):
        order_update_cb["cb"] = cb

    mock_dhan.set_order_update_callback = MagicMock(side_effect=_set_order_update_callback)

    call_counter = {"i": 0}

    def slow_place_order(**kwargs):
        time.sleep(0.25)  # simulate REST latency
        call_counter["i"] += 1
        return {"status": "success", "orderId": str(call_counter["i"])}

    mock_dhan.place_order = MagicMock(side_effect=slow_place_order)

    engine = LadderEngine(mock_dhan)
    engine.running = True
    engine.is_market_hours = MagicMock(return_value=True)
    engine.update_settings(StrategySettings(max_concurrent_orders=1, trade_capital=1000.0))

    stock = StockStatus(
        symbol="TST",
        mode="NONE",
        ltp=100.0,
        change_pct=1.0,
        pnl=0.0,
        status="IDLE",
        entry_price=0.0,
        quantity=0,
        ladder_level=0,
        next_add_on=0.0,
        stop_loss=0.0,
        target=0.0,
        prev_close=99.0,
        turnover=2_00_00_000.0,
    )
    engine.active_stocks = {"TST": stock}

    # Enqueue start; should return immediately (no REST blocking)
    t0 = time.time()
    engine.start_long_ladder(stock)
    assert (time.time() - t0) < 0.05, "start_long_ladder should not block"
    assert stock.pending_order == "START_LONG"

    # Tick should also return quickly (it will see pending_order and skip trading actions)
    t1 = time.time()
    engine.process_tick("TST", 100.5, 1000.0)
    assert (time.time() - t1) < 0.05, "process_tick should not block on REST orders"

    # Wait for worker to complete
    deadline = time.time() + 3.0
    while time.time() < deadline:
        # Simulate Live Order Update websocket fill (TRADED) once order_id is known.
        if order_update_cb["cb"] is not None and engine.active_stocks["TST"].pending_order == "START_LONG":
            # The test orderId will be "1" based on our slow_place_order counter.
            order_update_cb["cb"](
                {
                    "Type": "order_alert",
                    "Data": {
                        "orderNo": "1",
                        "status": "TRADED",
                        "tradedQty": 10,
                        "avgTradedPrice": 100.0,
                    },
                }
            )
        if engine.active_stocks["TST"].mode == "LONG" and engine.active_stocks["TST"].pending_order == "":
            break
        time.sleep(0.05)

    assert engine.active_stocks["TST"].mode == "LONG"
    assert engine.active_stocks["TST"].pending_order == ""


if __name__ == "__main__":
    test_start_order_is_async_and_tick_not_blocked()
    print("OK")
