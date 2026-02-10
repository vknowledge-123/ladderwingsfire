from unittest.mock import MagicMock

from config import StockStatus
from dhan_client import DhanClientWrapper
from strategy_engine import LadderEngine


def test_close_and_flip_uses_single_reverse_order_and_sets_next_qty():
    mock_dhan = MagicMock(spec=DhanClientWrapper)
    mock_dhan.is_connected = True

    engine = LadderEngine(mock_dhan)
    engine.running = True

    stock = StockStatus(
        symbol="TST",
        mode="LONG",
        ltp=100.0,
        change_pct=0.0,
        pnl=0.0,
        status="ACTIVE",
        entry_price=100.0,
        quantity=300,
        ladder_level=3,
        next_add_on=0.0,
        stop_loss=99.0,
        target=110.0,
        prev_close=99.0,
    )
    engine.active_stocks = {stock.symbol: stock}

    engine._place_market_order = MagicMock(
        return_value=({"status": "success", "orderId": "OID1"}, "OID1", 101.25, 400)
    )

    task = {
        "kind": "CLOSE_AND_FLIP",
        "symbol": "TST",
        "pending": "CLOSE_AND_FLIP_SHORT",
        "close_transaction_type": "SELL",
        "close_qty": 300,
        "open_qty": 100,
        "reverse_transaction_type": "SELL",
        "reverse_qty": 400,
        "flip_to": "SHORT",
        "price": 100.0,
        "cycle_index_next": 1,
        "gen": engine._order_generation,
    }
    stock.pending_order = "CLOSE_AND_FLIP_SHORT"

    engine._execute_order_task(task)

    engine._place_market_order.assert_called_once()
    called_args = engine._place_market_order.call_args[0]
    assert called_args[0] == "TST"
    assert called_args[1] == "SELL"
    assert called_args[2] == 400

    assert stock.mode == "SHORT"
    assert stock.quantity == 100
    assert stock.entry_price == 101.25
    assert stock.avg_entry_price == 101.25
    assert stock.ladder_level == 1
    assert stock.pending_order == ""


def test_place_market_order_inferrs_executed_price_from_positions():
    mock_dhan = MagicMock(spec=DhanClientWrapper)
    mock_dhan.is_connected = True

    before = [{"tradingSymbol": "TST", "buyQty": 100, "buyAvg": 100.0, "sellQty": 0, "sellAvg": 0.0}]
    after = [{"tradingSymbol": "TST", "buyQty": 150, "buyAvg": 101.0, "sellQty": 0, "sellAvg": 0.0}]

    calls = {"n": 0}

    def get_positions_side_effect():
        calls["n"] += 1
        return before if calls["n"] == 1 else after

    mock_dhan.get_positions = MagicMock(side_effect=get_positions_side_effect)
    mock_dhan.place_order = MagicMock(return_value={"status": "success", "orderId": "OID2"})

    engine = LadderEngine(mock_dhan)
    resp, order_id, exec_price, exec_qty = engine._place_market_order("TST", "BUY", 50, 100.5)

    assert resp.get("status") == "success"
    assert order_id == "OID2"
    assert exec_qty == 50
    # From before->after: delta value = (101*150) - (100*100) = 5150; delta qty=50 => 103.0
    # But buyAvg in 'after' is 101.0; expected fill price computed by delta -> 103.0
    assert abs(exec_price - 103.0) < 1e-6


if __name__ == "__main__":
    test_close_and_flip_uses_single_reverse_order_and_sets_next_qty()
    test_place_market_order_inferrs_executed_price_from_positions()
    print("OK")

