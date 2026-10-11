"""Synthetic order generation for ChronosMatch simulations."""

from __future__ import annotations

import random
from dataclasses import dataclass

from chronosmatch.ipc.order_protocol import Order, OrderSide


@dataclass(frozen=True, slots=True)
class OrderGenerator:
    """Generate reproducible market orders using a seeded RNG."""

    seed: int | None = None
    min_price: int = 100_000
    max_price: int = 110_000
    min_quantity: int = 1
    max_quantity: int = 100
    order_count: int = 1
    buy_ratio: float = 0.5

    def __post_init__(self) -> None:
        if self.min_price > self.max_price:
            raise ValueError("min_price must be <= max_price")
        if self.min_quantity <= 0 or self.max_quantity < self.min_quantity:
            raise ValueError("Invalid quantity bounds")
        if self.order_count < 0:
            raise ValueError("order_count must be non-negative")
        if not 0.0 <= self.buy_ratio <= 1.0:
            raise ValueError("buy_ratio must be between 0 and 1")

    def generate(self, order_id: int) -> Order:
        """Create one synthetic order for the given identifier."""
        rng = random.Random(self.seed + order_id if self.seed is not None else None)
        side = (
            OrderSide.BUY
            if rng.random() < self.buy_ratio
            else OrderSide.SELL
        )
        price = rng.randint(self.min_price, self.max_price)
        quantity = rng.randint(self.min_quantity, self.max_quantity)
        return Order(
            order_id=order_id,
            price=price,
            quantity=quantity,
            side=side,
        )

    def generate_batch(self) -> list[Order]:
        """Create a batch of synthetic orders."""
        return [self.generate(order_id) for order_id in range(1, self.order_count + 1)]
