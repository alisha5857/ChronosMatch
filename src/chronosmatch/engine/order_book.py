
"""Price-level order book with FIFO priority at each price."""

from collections import deque
from dataclasses import dataclass

from chronosmatch.ipc.order_protocol import Order, OrderSide


@dataclass(slots=True)
class RestingOrder:
    """An order waiting in the order book."""

    order_id: int
    price: int
    remaining_quantity: int
    side: OrderSide
    sequence: int

    @classmethod
    def from_order(
        cls,
        order: Order,
        sequence: int,
    ) -> "RestingOrder":
        """Create a resting order from an incoming order."""
        return cls(
            order_id=order.order_id,
            price=order.price,
            remaining_quantity=order.quantity,
            side=order.side,
            sequence=sequence,
        )


class OrderBook:
    """Maintain buy and sell orders using price-time priority."""

    def __init__(self) -> None:
        self.bids: dict[int, deque[RestingOrder]] = {}
        self.asks: dict[int, deque[RestingOrder]] = {}

    def _levels(
        self,
        side: OrderSide,
    ) -> dict[int, deque[RestingOrder]]:
        """Return price levels for the requested side."""
        side = OrderSide(side)
        return self.bids if side == OrderSide.BUY else self.asks

    def add(self, order: RestingOrder) -> None:
        """Add an order to the back of its price-level queue."""
        if order.remaining_quantity <= 0:
            raise ValueError("Remaining quantity must be positive")

        levels = self._levels(order.side)
        levels.setdefault(order.price, deque()).append(order)

    def best_order(
        self,
        side: OrderSide,
    ) -> RestingOrder | None:
        """Return the highest-priority live order on one side."""
        levels = self._levels(side)

        while levels:
            price = (
                max(levels)
                if OrderSide(side) == OrderSide.BUY
                else min(levels)
            )
            queue = levels[price]

            while queue and queue[0].remaining_quantity <= 0:
                queue.popleft()

            if queue:
                return queue[0]

            del levels[price]

        return None

    def remove_best_order(self, side: OrderSide) -> RestingOrder:
        """Remove and return the best order on one side."""
        order = self.best_order(side)

        if order is None:
            raise IndexError("No resting order on this side")

        levels = self._levels(side)
        queue = levels[order.price]
        removed = queue.popleft()

        if not queue:
            del levels[order.price]

        return removed

    def cancel(self, order_id: int) -> bool:
        """Cancel a resting order by ID."""
        for levels in (self.bids, self.asks):
            for price, queue in list(levels.items()):
                for order in queue:
                    if order.order_id == order_id:
                        queue.remove(order)

                        if not queue:
                            del levels[price]

                        return True

        return False

    def best_bid(self) -> int | None:
        """Return the highest live buy price, if any."""
        best = self.best_order(OrderSide.BUY)
        return best.price if best is not None else None

    def best_ask(self) -> int | None:
        """Return the lowest live sell price, if any."""
        best = self.best_order(OrderSide.SELL)
        return best.price if best is not None else None

    def spread(self) -> int | None:
        """Return the ask-minus-bid spread, if both sides exist."""
        bid = self.best_bid()
        ask = self.best_ask()

        if bid is None or ask is None:
            return None

        return ask - bid

    def order_count(self) -> int:
        """Return the number of live resting orders."""
        return sum(
            1
            for levels in (self.bids, self.asks)
            for queue in levels.values()
            for order in queue
            if order.remaining_quantity > 0
        )

    def snapshot(self) -> dict[str, list[tuple[int, int]]]:
        """Return aggregated price levels for display."""
        def aggregate(
            levels: dict[int, deque[RestingOrder]],
            descending: bool,
        ) -> list[tuple[int, int]]:
            result = []

            for price in sorted(levels, reverse=descending):
                quantity = sum(
                    order.remaining_quantity
                    for order in levels[price]
                    if order.remaining_quantity > 0
                )

                if quantity > 0:
                    result.append((price, quantity))

            return result

        return {
            "bids": aggregate(self.bids, descending=True),
            "asks": aggregate(self.asks, descending=False),
        }
