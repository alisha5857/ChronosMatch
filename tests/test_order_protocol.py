import pytest

from chronosmatch.ipc.order_protocol import (
    ORDER_SIZE,
    Order,
    OrderSide,
    decode_order,
    encode_order,
)


def test_order_round_trip():
    original = Order(
        order_id=101,
        price=185250,
        quantity=10,
        side=OrderSide.BUY,
    )

    encoded = encode_order(original)
    decoded = decode_order(encoded)

    assert len(encoded) == ORDER_SIZE == 32
    assert decoded == original


def test_sell_order_round_trip():
    original = Order(202, 185300, 5, OrderSide.SELL)

    assert decode_order(encode_order(original)) == original


def test_rejects_zero_quantity():
    with pytest.raises(ValueError):
        Order(1, 100, 0, OrderSide.BUY)


from chronosmatch.ipc.order_protocol import OrderSide

side=OrderSide.BUY


def test_rejects_wrong_record_size():
    with pytest.raises(ValueError):
        decode_order(b"\x00\x01")


def test_rejects_negative_order_id():
    with pytest.raises(ValueError):
        Order(-1, 100, 1, OrderSide.BUY)
