import multiprocessing
import uuid

import pytest

from chronosmatch.ipc.order_protocol import (
    ORDER_SIZE,
    Order,
    OrderSide,
    encode_order,
)
from chronosmatch.ipc.ring_buffer import SharedMemoryRingBuffer


@pytest.fixture
def buffer_factory():
    buffers = []

    def create_buffer(capacity: int = 4) -> SharedMemoryRingBuffer:
        name = f"chronos_test_{uuid.uuid4().hex}"
        buffer = SharedMemoryRingBuffer(name, capacity=capacity, create=True)
        buffers.append(buffer)
        return buffer

    yield create_buffer

    for buffer in reversed(buffers):
        try:
            if not buffer.closed:
                buffer.close()
        finally:
            if buffer.owner:
                try:
                    buffer.unlink()
                except FileNotFoundError:
                    pass


def _write_records(buffer_name: str, records: tuple[bytes, ...]) -> None:
    buffer = SharedMemoryRingBuffer(buffer_name, create=False)
    try:
        for record in records:
            if not buffer.write(record):
                raise RuntimeError("Unexpected full ring buffer")
    finally:
        buffer.close()


def make_record(order_id: int) -> bytes:
    return encode_order(
        Order(
            order_id=order_id,
            price=100_000 + order_id,
            quantity=order_id + 1,
            side=OrderSide.BUY if order_id % 2 else OrderSide.SELL,
        )
    )


def test_write_and_read_valid_32_byte_record(buffer_factory):
    buffer = buffer_factory()
    record = make_record(1)

    assert len(record) == ORDER_SIZE == 32
    assert buffer.write(record) is True
    assert buffer.read() == record


def test_empty_buffer_read_returns_none(buffer_factory):
    buffer = buffer_factory()

    assert buffer.is_empty is True
    assert buffer.read() is None


def test_full_buffer_write_returns_false(buffer_factory):
    buffer = buffer_factory(capacity=2)

    assert buffer.write(make_record(1)) is True
    assert buffer.write(make_record(2)) is True
    assert buffer.is_full is True
    assert buffer.write(make_record(3)) is False


def test_fifo_order_is_preserved_across_wraparound(buffer_factory):
    buffer = buffer_factory(capacity=2)
    first = make_record(1)
    second = make_record(2)
    third = make_record(3)

    assert buffer.write(first) is True
    assert buffer.write(second) is True
    assert buffer.read() == first
    assert buffer.write(third) is True
    assert buffer.read() == second
    assert buffer.read() == third
    assert buffer.read() is None


@pytest.mark.parametrize(
    "record",
    [b"", b"\x00" * (ORDER_SIZE - 1), b"\x00" * (ORDER_SIZE + 1)],
)
def test_write_rejects_invalid_record_sizes(buffer_factory, record):
    buffer = buffer_factory()

    with pytest.raises(ValueError, match=f"Record must be {ORDER_SIZE} bytes"):
        buffer.write(record)


def test_close_disables_access_and_only_creator_can_unlink(buffer_factory):
    creator = buffer_factory()
    reader = SharedMemoryRingBuffer(creator.name, create=False)

    try:
        with pytest.raises(PermissionError, match="Only the creator"):
            reader.unlink()
    finally:
        reader.close()

    creator.close()
    assert creator.closed is True

    with pytest.raises(RuntimeError, match="Buffer is closed"):
        creator.read()
    with pytest.raises(RuntimeError, match="Buffer is closed"):
        creator.write(make_record(1))


def test_separate_process_can_write_records_for_parent_to_read(buffer_factory):
    buffer = buffer_factory(capacity=3)
    expected = (make_record(1), make_record(2), make_record(3))
    context = multiprocessing.get_context("spawn")
    process = context.Process(
        target=_write_records,
        args=(buffer.name, expected),
    )

    process.start()
    process.join(timeout=10)
    if process.is_alive():
        process.terminate()
        process.join()
        pytest.fail("Producer process did not finish")

    assert process.exitcode == 0
    assert [buffer.read() for _ in expected] == list(expected)
    assert buffer.read() is None