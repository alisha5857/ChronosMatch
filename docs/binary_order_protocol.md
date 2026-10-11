
# ChronosMatch — Binary Order Protocol

## 1. Purpose

The binary order protocol defines a fixed-size representation of an order so that components can exchange order data without JSON or pickle serialization.

Implementation:
`src/chronosmatch/ipc/order_protocol.py`

## 2. Binary Record Layout

The record uses the Python struct format:

```python
struct.Struct("<QqQB7x")
```

The format uses little-endian byte order and standard field sizes without native alignment.

| Field | Type | Size | Meaning |
|---|---|---:|---|
| order_id | Q | 8 bytes | Unsigned 64-bit order ID |
| price | q | 8 bytes | Signed 64-bit integer price |
| quantity | Q | 8 bytes | Unsigned 64-bit quantity |
| side | B | 1 byte | 0 = BUY, 1 = SELL |
| reserved | 7x | 7 bytes | Padding/reserved space |
| Total | | 32 bytes | Fixed-size record |

The padding bytes are reserved and do not contain meaningful order fields.

## 3. Why Fixed-Size Records?

Fixed-size records provide:
- Predictable record boundaries.
- Simple offset calculations.
- No need to parse variable-length JSON.
- A straightforward layout for memory-mapped buffers.
- A format that can be reproduced in other languages.

A binary representation does not automatically make a system zero-copy. Converting objects to bytes may still allocate memory and copy data.

## 4. Order Side

```python
from enum import IntEnum

class OrderSide(IntEnum):
    BUY = 0
    SELL = 1
```

Only these two side values are valid.

## 5. Example

```python
from chronosmatch.ipc.order_protocol import (
    Order, OrderSide, encode_order, decode_order
)

order = Order(
    order_id=101,
    price=185250,
    quantity=10,
    side=OrderSide.BUY,
)

encoded = encode_order(order)
decoded = decode_order(encoded)

assert len(encoded) == 32
assert decoded == order
```

The example uses integer prices. For a real market, the application must define a price scale, such as integer units representing a fixed number of currency subunits. Floating-point prices should not be introduced without a deliberate protocol change.

## 6. Validation Rules

- `order_id` must fit in an unsigned 64-bit integer.
- `price` must fit in a signed 64-bit integer.
- `quantity` must be greater than zero and fit in an unsigned 64-bit integer.
- `side` must be BUY or SELL.
- A decoded record must contain exactly 32 bytes.
- Invalid side values must be rejected.

Additional business rules, including valid price ranges and duplicate order IDs, belong to higher-level validation.

## 7. Encoding and Decoding

Encoding packs the fields into the defined binary structure.

Decoding unpacks a record and validates its side value and field constraints.

For a ring buffer, the producer must write a complete record before publishing the write position. The consumer must not read a record before the producer has published it.

## 8. Testing

Tests are located in:

`tests/test_order_protocol.py`

They should cover:
- BUY and SELL round trips.
- Record length.
- Zero quantity rejection.
- Invalid side rejection.
- Negative order ID rejection.
- Incorrect record size.
- Numeric boundary values.

## 9. Protocol Compatibility

The format string, field order, byte order, and sizes form the protocol contract.

Changing any of these requires coordinated changes to producers, consumers, tests, and any Cython or other-language implementation that reads the records.

## 10. Limitations

The protocol does not define:
- Network transport.
- Authentication or encryption.
- Multi-process synchronization.
- Order-book rules.
- Trade persistence.
- Market-specific tick sizes.

Those responsibilities belong to other components.
