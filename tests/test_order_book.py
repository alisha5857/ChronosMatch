import pytest

from chronosmatch.engine.order_book import OrderBook, RestingOrder
from chronosmatch.ipc.order_protocol import OrderSide


def resting_order(
    order_id: int,
    price: int,
    quantity: int,
    side: OrderSide,
    sequence: int,
) -> RestingOrder:
    return RestingOrder(
        order_id=order_id,
        price=price,
        remaining_quantity=quantity,
        side=side,
        sequence=sequence,
    )


def test_best_order_uses_price_priority_and_fifo_within_level():
    book = OrderBook()
    first_at_price = resting_order(1, 100, 4, OrderSide.BUY, 1)
    second_at_price = resting_order(2, 100, 3, OrderSide.BUY, 2)
    better_price = resting_order(3, 101, 2, OrderSide.BUY, 3)
    higher_ask = resting_order(4, 104, 1, OrderSide.SELL, 4)
    better_ask = resting_order(5, 103, 1, OrderSide.SELL, 5)

    for order in (first_at_price, second_at_price, better_price, higher_ask, better_ask):
        book.add(order)

    assert book.best_order(OrderSide.BUY) is better_price
    assert book.best_order(OrderSide.SELL) is better_ask

    book.cancel(better_price.order_id)
    assert book.best_order(OrderSide.BUY) is first_at_price
    assert book.remove_best_order(OrderSide.BUY) is first_at_price
    assert book.best_order(OrderSide.BUY) is second_at_price


def test_snapshot_aggregates_quantities_and_orders_prices():
    book = OrderBook()
    book.add(resting_order(1, 100, 4, OrderSide.BUY, 1))
    book.add(resting_order(2, 100, 3, OrderSide.BUY, 2))
    book.add(resting_order(3, 98, 5, OrderSide.BUY, 3))
    book.add(resting_order(4, 104, 2, OrderSide.SELL, 4))
    book.add(resting_order(5, 102, 6, OrderSide.SELL, 5))

    assert book.snapshot() == {
        "bids": [(100, 7), (98, 5)],
        "asks": [(102, 6), (104, 2)],
    }
    assert book.best_bid() == 100
    assert book.best_ask() == 102
    assert book.spread() == 2
    assert book.order_count() == 5


def test_cancel_removes_matching_order_and_empty_price_level():
    book = OrderBook()
    order = resting_order(1, 100, 4, OrderSide.BUY, 1)
    book.add(order)

    assert book.cancel(order.order_id) is True
    assert book.cancel(order.order_id) is False
    assert book.best_bid() is None
    assert book.snapshot() == {"bids": [], "asks": []}
    assert book.order_count() == 0


def test_add_rejects_non_positive_remaining_quantity():
    book = OrderBook()

    with pytest.raises(ValueError, match="Remaining quantity must be positive"):
        book.add(resting_order(1, 100, 0, OrderSide.BUY, 1))


def test_remove_best_order_raises_when_side_is_empty():
    book = OrderBook()

    with pytest.raises(IndexError, match="No resting order on this side"):
        book.remove_best_order(OrderSide.SELL)