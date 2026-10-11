"""Fixed-size binary order protocol."""

from dataclasses import dataclass
from enum import IntEnum
import struct


class OrderSide(IntEnum):
    BUY = 0
    SELL = 1


# Order ID: 8 bytes
# Price: 8 bytes
# Quantity: 8 bytes
# Side: 1 byte
# Padding: 7 bytes
ORDER_STRUCT = struct.Struct("<QqQB7x")
ORDER_SIZE = ORDER_STRUCT.size  # 32 bytes


@dataclass(frozen=True, slots=True)
class Order:
    order_id: int
    price: int
    quantity: int
    side: OrderSide

    def __post_init__(self):
        if isinstance(self.order_id, bool) or not isinstance(self.order_id, int):
            raise ValueError("Order ID must be an integer")
        if not 0 <= self.order_id <= 2**64 - 1:
            raise ValueError("Order ID must be an unsigned 64-bit integer")

        if isinstance(self.price, bool) or not isinstance(self.price, int):
            raise ValueError("Price must be an integer")
        if not -(2**63) <= self.price <= 2**63 - 1:
            raise ValueError("Price is outside the supported range")

        if isinstance(self.quantity, bool) or not isinstance(self.quantity, int):
            raise ValueError("Quantity must be an integer")
        if not 1 <= self.quantity <= 2**64 - 1:
            raise ValueError("Quantity must be positive")

        try:
            object.__setattr__(self, "side", OrderSide(self.side))
        except (TypeError, ValueError) as exc:
            raise ValueError("Side must be BUY (0) or SELL (1)") from exc


def encode_order(order: Order) -> bytes:
    """Convert an Order into a 32-byte binary record."""
    if not isinstance(order, Order):
        raise TypeError("Expected an Order instance")

    return ORDER_STRUCT.pack(
        order.order_id,
        order.price,
        order.quantity,
        int(order.side),
    )


def decode_order(data: bytes | bytearray | memoryview) -> Order:
    """Convert a 32-byte binary record back into an Order."""
    if len(data) != ORDER_SIZE:
        raise ValueError(f"Expected {ORDER_SIZE} bytes")

    order_id, price, quantity, side = ORDER_STRUCT.unpack(data)

    return Order(
        order_id=order_id,
        price=price,
        quantity=quantity,
        side=side,
    )