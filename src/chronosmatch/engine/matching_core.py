"""Python fallback implementation for the matching core.

This module mirrors the Cython API used by the project so the engine can run even
when the extension has not yet been built.
"""

BUY = 0
SELL = 1
IS_COMPILED = False


def prices_cross(incoming_price: int, incoming_side: int, resting_price: int) -> bool:
    """Return whether an incoming order crosses the resting price."""
    if incoming_side == BUY:
        return incoming_price >= resting_price
    if incoming_side == SELL:
        return incoming_price <= resting_price
    raise ValueError("incoming_side must be BUY (0) or SELL (1)")


def fill_quantity(incoming_quantity: int, resting_quantity: int) -> int:
    """Return the executable quantity for two orders."""
    if incoming_quantity == 0 or resting_quantity == 0:
        return 0
    if incoming_quantity < resting_quantity:
        return incoming_quantity
    return resting_quantity
