from pathlib import Path
import runpy

from chronosmatch.engine import matching_core
from chronosmatch.engine.matching_engine import MatchingEngine, Trade
from chronosmatch.ipc.order_protocol import Order, OrderSide


def order(
    order_id: int,
    price: int,
    quantity: int,
    side: OrderSide,
) -> Order:
    return Order(
        order_id=order_id,
        price=price,
        quantity=quantity,
        side=side,
    )


def test_crossing_orders_trade_at_resting_price():
    engine = MatchingEngine()
    assert engine.process_order(order(1, 100, 5, OrderSide.SELL)) == []

    trades = engine.process_order(order(2, 105, 3, OrderSide.BUY))

    assert trades == [
        Trade(
            trade_id=1,
            buy_order_id=2,
            sell_order_id=1,
            price=100,
            quantity=3,
        )
    ]
    assert engine.get_trades() == trades
    assert engine.get_order_book() == {"bids": [], "asks": [(100, 2)]}


def test_matching_uses_best_price_then_fifo_time_priority():
    engine = MatchingEngine()
    engine.process_order(order(1, 102, 2, OrderSide.SELL))
    engine.process_order(order(2, 101, 3, OrderSide.SELL))
    engine.process_order(order(3, 101, 4, OrderSide.SELL))

    trades = engine.process_order(order(4, 103, 5, OrderSide.BUY))

    assert trades == [
        Trade(1, 4, 2, 101, 3),
        Trade(2, 4, 3, 101, 2),
    ]
    assert engine.get_order_book() == {
        "bids": [],
        "asks": [(101, 2), (102, 2)],
    }


def test_partial_fill_leaves_resting_quantity_available():
    engine = MatchingEngine()
    engine.process_order(order(1, 100, 8, OrderSide.BUY))

    trades = engine.process_order(order(2, 99, 3, OrderSide.SELL))

    assert trades == [Trade(1, 1, 2, 100, 3)]
    assert engine.get_order_book() == {"bids": [(100, 5)], "asks": []}


def test_non_crossing_order_rests_without_trade():
    engine = MatchingEngine()
    engine.process_order(order(1, 100, 4, OrderSide.BUY))

    trades = engine.process_order(order(2, 101, 6, OrderSide.SELL))

    assert trades == []
    assert engine.get_trades() == []
    assert engine.get_order_book() == {
        "bids": [(100, 4)],
        "asks": [(101, 6)],
    }


def test_cancel_order_removes_resting_order_without_trade():
    engine = MatchingEngine()
    engine.process_order(order(1, 100, 4, OrderSide.BUY))

    assert engine.cancel_order(1) is True
    assert engine.cancel_order(1) is False
    assert engine.get_order_book() == {"bids": [], "asks": []}
    assert engine.get_trades() == []


def test_sell_aggressor_matches_bid_at_resting_bid_price():
    engine = MatchingEngine()
    engine.process_order(order(1, 100, 4, OrderSide.BUY))

    trades = engine.process_order(order(2, 95, 4, OrderSide.SELL))

    assert trades == [Trade(1, 1, 2, 100, 4)]
    assert engine.get_order_book() == {"bids": [], "asks": []}


def test_matching_uses_active_core_helpers_with_python_engine_parity(monkeypatch):
    assert isinstance(matching_core.IS_COMPILED, bool)

    crossed_prices = []
    fill_quantities = []
    prices_cross = matching_core.prices_cross
    fill_quantity = matching_core.fill_quantity

    def track_prices_cross(incoming_price, incoming_side, resting_price):
        crossed_prices.append((incoming_price, incoming_side, resting_price))
        return prices_cross(incoming_price, incoming_side, resting_price)

    def track_fill_quantity(incoming_quantity, resting_quantity):
        fill_quantities.append((incoming_quantity, resting_quantity))
        return fill_quantity(incoming_quantity, resting_quantity)

    monkeypatch.setattr(matching_core, "prices_cross", track_prices_cross)
    monkeypatch.setattr(matching_core, "fill_quantity", track_fill_quantity)

    engine = MatchingEngine()
    engine.process_order(order(1, 100, 5, OrderSide.SELL))
    trades = engine.process_order(order(2, 105, 3, OrderSide.BUY))

    assert crossed_prices == [(105, int(OrderSide.BUY), 100)]
    assert fill_quantities == [(3, 5)]
    assert trades == [Trade(1, 2, 1, 100, 3)]
    assert engine.get_order_book() == {"bids": [], "asks": [(100, 2)]}


def test_active_core_matches_python_fallback_results(monkeypatch):
    fallback = runpy.run_path(
        str(
            Path(__file__).resolve().parents[1]
            / "src"
            / "chronosmatch"
            / "engine"
            / "matching_core.py"
        )
    )

    for incoming_price, side, resting_price in (
        (100, OrderSide.BUY, 100),
        (99, OrderSide.BUY, 100),
        (100, OrderSide.SELL, 100),
        (101, OrderSide.SELL, 100),
    ):
        assert matching_core.prices_cross(
            incoming_price,
            int(side),
            resting_price,
        ) == fallback["prices_cross"](
            incoming_price,
            int(side),
            resting_price,
        )

    for incoming_quantity, resting_quantity in (
        (0, 0),
        (0, 4),
        (3, 5),
        (5, 3),
        (2**64 - 1, 2**64 - 1),
    ):
        assert matching_core.fill_quantity(
            incoming_quantity,
            resting_quantity,
        ) == fallback["fill_quantity"](
            incoming_quantity,
            resting_quantity,
        )

    def run_scenario():
        engine = MatchingEngine()
        engine.process_order(order(1, 102, 2, OrderSide.SELL))
        engine.process_order(order(2, 101, 3, OrderSide.SELL))
        engine.process_order(order(3, 101, 4, OrderSide.SELL))
        generated = engine.process_order(order(4, 103, 5, OrderSide.BUY))
        return generated, engine.get_trades(), engine.get_order_book()

    active_results = run_scenario()

    monkeypatch.setattr(matching_core, "prices_cross", fallback["prices_cross"])
    monkeypatch.setattr(
        matching_core,
        "fill_quantity",
        fallback["fill_quantity"],
    )

    assert run_scenario() == active_results