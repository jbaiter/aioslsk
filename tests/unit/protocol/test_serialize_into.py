from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from aioslsk.protocol.messages import PeerDirectoryContentsReply, PeerSearchReply, PeerSharesReply
from aioslsk.protocol.primitives import (
    Attribute,
    DirectoryData,
    FileData,
    MessageDataclass,
    array,
    string,
    uint32,
)


def _serialize_into_bytes(obj, **kwargs) -> bytes:
    buffer = bytearray()
    if kwargs:
        obj.serialize_into(buffer, **kwargs)
    else:
        obj.serialize_into(buffer)
    return bytes(buffer)


def test_protocol_dataclasses_serialize_into_matches_serialize():
    attr = Attribute(0, 128)
    fd = FileData(
        unknown=1,
        filename="song.mp3",
        filesize=123,
        extension="mp3",
        attributes=[attr],
    )
    dd = DirectoryData(name="@@abc", files=[fd])

    for obj in (attr, fd, dd):
        assert obj.serialize() == _serialize_into_bytes(obj)


def test_message_dataclass_serialize_into_matches_serialize():
    @dataclass(order=True, slots=True)
    class Msg(MessageDataclass):
        MESSAGE_ID = uint32(0x12345678)
        foo: int = field(metadata={"type": uint32})
        bar: str = field(metadata={"type": string})

    msg = Msg(foo=7, bar="abc")
    assert msg.serialize() == _serialize_into_bytes(msg)
    assert msg.serialize(compress=True) == _serialize_into_bytes(msg, compress=True)


@pytest.mark.parametrize(
    "msg",
    [
        PeerSharesReply.Request(
            directories=[
                DirectoryData(
                    name="@@abc",
                    files=[
                        FileData(
                            unknown=1,
                            filename="song.mp3",
                            filesize=123,
                            extension="mp3",
                            attributes=[Attribute(0, 128)],
                        )
                    ],
                )
            ],
            locked_directories=[],
        ),
        PeerSearchReply.Request(
            username="user0",
            ticket=1,
            results=[],
            has_slots_free=True,
            avg_speed=0,
            queue_size=0,
            unknown=0,
            locked_results=None,
        ),
        PeerDirectoryContentsReply.Request(
            ticket=1,
            directory="@@abc",
            directories=[],
        ),
    ],
)
def test_compress_by_default_messages_serialize_into_matches_serialize(msg):
    # These messages define serialize(compress=True) overrides. serialize_into()
    # should match that behavior by default.
    assert msg.serialize() == _serialize_into_bytes(msg)
    assert msg.serialize(compress=False) == _serialize_into_bytes(msg, compress=False)


def test_array_serialize_into_allows_dataclass_without_serialize_into():
    @dataclass
    class Bare:
        value: int

        def serialize(self) -> bytes:
            return uint32(self.value).serialize()

    arr = array([Bare(1), Bare(2)])
    expected = uint32(2).serialize() + uint32(1).serialize() + uint32(2).serialize()
    assert arr.serialize(Bare) == expected

    buffer = bytearray()
    arr.serialize_into(buffer, Bare)
    assert bytes(buffer) == expected
