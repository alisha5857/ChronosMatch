
"""Price-time priority matching engine."""

from dataclasses import dataclass

from . import matching_core
from chronosmatch.engine.order_book import OrderBook, RestingOrder
from chronosmatch.ipc.order_protocol import Order, OrderSide


@dataclass(frozen=True, slots=True)
class Trade:
    """A completed trade between a buyer and a seller."""

    trade_id: int
    buy_order_id: int
    sell_order_id: int
    price: int
    quantity: int


class MatchingEngine:
    """Single-threaded limit-order matching engine."""

    def __init__(self) -> None:
        self.order_book = OrderBook()
        self._sequence = 0
        self._trade_sequence = 0
        self._seen_order_ids: set[int] = set()
        self.trades: list[Trade] = []

    @staticmethod
    def _crosses(
        incoming: Order,
        resting: RestingOrder,
    ) -> bool:
        """Check whether two orders can trade at the resting price."""
        return matching_core.prices_cross(
            incoming.price,
            int(incoming.side),
            resting.price,
        )

    def process_order(self, order: Order) -> list[Trade]:
        """Process an order and return all trades it generates."""
        if not isinstance(order, Order):
            raise TypeError("order must be an Order instance")

        if order.order_id in self._seen_order_ids:
            raise ValueError(f"Duplicate order ID: {order.order_id}")

        self._seen_order_ids.add(order.order_id)

        remaining = order.quantity
        generated: list[Trade] = []

        opposite_side = (
            OrderSide.SELL
            if order.side == OrderSide.BUY
            else OrderSide.BUY
        )

        while remaining > 0:
            resting = self.order_book.best_order(opposite_side)

            if resting is None or not self._crosses(order, resting):
                break

            quantity = matching_core.fill_quantity(
                remaining,
                resting.remaining_quantity,
            )

            if quantity == resting.remaining_quantity:
                self.order_book.remove_best_order(opposite_side)

            resting.remaining_quantity -= quantity
            remaining -= quantity

            self._trade_sequence += 1

            if order.side == OrderSide.BUY:
                buy_order_id = order.order_id
                sell_order_id = resting.order_id
            else:
                buy_order_id = resting.order_id
                sell_order_id = order.order_id

            trade = Trade(
                trade_id=self._trade_sequence,
                buy_order_id=buy_order_id,
                sell_order_id=sell_order_id,
                price=resting.price,
                quantity=quantity,
            )

            generated.append(trade)
            self.trades.append(trade)

        # Any unfilled quantity rests in the book.
        if remaining > 0:
            self._sequence += 1

            resting_order = RestingOrder(
                order_id=order.order_id,
                price=order.price,
                remaining_quantity=remaining,
                side=order.side,
                sequence=self._sequence,
            )

            self.order_book.add(resting_order)

        return generated

    def cancel_order(self, order_id: int) -> bool:
        """Cancel an order that is still resting in the book."""
        return self.order_book.cancel(order_id)

    def get_order_book(self) -> dict[str, list[tuple[int, int]]]:
        """Return the aggregated order-book snapshot."""
        return self.order_book.snapshot()

    def get_trades(self) -> list[Trade]:
        """Return a copy of the trade history."""
        return list(self.trades)
