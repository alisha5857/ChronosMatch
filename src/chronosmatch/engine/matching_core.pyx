
# cython: language_level=3, boundscheck=False, wraparound=False

"""Typed helpers for the ChronosMatch matching core."""

cdef int BUY = 0
cdef int SELL = 1
IS_COMPILED = True


cpdef bint prices_cross(
    long long incoming_price,
    int incoming_side,
    long long resting_price,
):
    """Return whether the incoming order crosses the resting price."""
    if incoming_side == BUY:
        return incoming_price >= resting_price

    if incoming_side == SELL:
        return incoming_price <= resting_price

    raise ValueError("incoming_side must be BUY (0) or SELL (1)")


cpdef unsigned long long fill_quantity(
    unsigned long long incoming_quantity,
    unsigned long long resting_quantity,
):
    """Return the executable quantity for two orders."""
    if incoming_quantity == 0 or resting_quantity == 0:
        return 0

    if incoming_quantity < resting_quantity:
        return incoming_quantity

    return resting_quantity
