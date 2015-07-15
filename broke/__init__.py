from collections import namedtuple
from contextlib import contextmanager
import os
import zlib
import io
import gzip
import time

import construct
import portalocker


BlockHeader = construct.Struct(
    "Header",
    construct.Magic(b"BrokeHeader"),
    construct.ULInt32("length"),
    construct.ULInt32("checksum"),
    construct.Flag("committed"),
    # reserve some space
    construct.Magic(b"\x00" * 16))

MessageHeader = construct.Struct(
    "MessageHeader",
    construct.Magic(b"\x00" * 8),
    construct.ULInt32("length"),
    construct.ULInt64("timestamp"),
    construct.String("topic", 64, padchar="\x00", encoding="ascii"))

EMPTY_HEADER = b"\x00" * BlockHeader.sizeof()

Message = namedtuple("Message", ["topic", "time", "payload"])

@contextmanager
def lock(fp, shared=False, timeout=5, retry_interval=0.25):
    start = time.time()
    while True:
        try:
            lock_type = portalocker.LOCK_SH if shared else portalocker.LOCK_EX
            portalocker.lock(fp, lock_type)
            try:
                yield fp
                return

            finally:
                portalocker.unlock(fp)

        except portalocker.LockException:
            if time.time() - start > timeout:
                raise TimeoutError("Could not lock file")

            # wait and try again
            time.sleep(retry_interval)


def read_fully(fp, length):
    result = fp.read(length)
    if len(result) != length:
        raise IOError("short read")

    return result


def _new_buffer():
    return gzip.GzipFile(fileobj=io.BytesIO(), mode="wb")


class BrokeWriter(object):
    def __init__(self, datafile):
        self.datafile = datafile

        self.raw_buffer = io.BytesIO()
        self.buffer = gzip.GzipFile(fileobj=self.raw_buffer, mode="wb")

    def store(self, topic, payload):
        if not isinstance(payload, bytes):
            raise IOError("Can only store data of type bytes")

        if not isinstance(topic, str):
            raise IOError("Topic must be str")

        header = construct.Container(length=len(payload), topic=topic, timestamp=int(time.time() * 1000))
        self.buffer.write(MessageHeader.build(header))
        self.buffer.write(payload)

    def rollback(self):
        self.raw_buffer = io.BytesIO()
        self.buffer = gzip.GzipFile(fileobj=self.raw_buffer, mode="wb")

    @property
    def dirty(self):
        return self.buffer.tell() > 0

    def commit(self):
        if not self.dirty:
            return

        # compress data in memory and reset the buffer
        self.buffer.close()
        compressed = self.raw_buffer.getvalue()
        self.rollback()

        # calculate header data
        checksum = _checksum(compressed)
        header = construct.Container(length=len(compressed), checksum=checksum, committed=False)
        header_bytes = BlockHeader.build(header)

        with open(self.datafile, mode="rb+") as fp, lock(fp):
            # write empty header and data
            fp.seek(0, os.SEEK_END)
            start = fp.tell()
            try:
                fp.write(header_bytes)
                fp.write(compressed)

                # now update the header
                fp.seek(start)
                header.committed = True
                fp.write(BlockHeader.build(header))

                # everything looks good, flush to disk!
                fp.flush()

            except:
                # noinspection PyBroadException
                try:
                    fp.truncate(start)
                except:
                    print("Could not truncate file")

                raise

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback()
        else:
            self.commit()


def _checksum(bytes):
    return zlib.crc32(bytes)


def iter_blocks(fp):
    while True:
        with lock(fp, shared=True):
            header_bytes = fp.read(BlockHeader.sizeof())
            if not header_bytes:
                break

            # read block header
            header = BlockHeader.parse(header_bytes)
            if header.length == 0:
                print("found empty block, skipping.")
                continue

            # stop if block is not written
            if not header.committed:
                print("found non-committed block, skipping.")
                fp.seek(header.length, os.SEEK_CUR)
                continue

            # read block and verify checksum
            block_bytes = read_fully(fp, header.length)
            if not _checksum(block_bytes) == header.checksum:
                raise IOError("checksum for block invalid")

        # everything is awesome
        yield block_bytes


def iter_messages(fp):
    while True:
        header_bytes = fp.read(MessageHeader.sizeof())
        if not header_bytes:
            break

        header = MessageHeader.parse(header_bytes)
        payload = read_fully(fp, header.length)
        yield Message(header.topic, header.timestamp, payload)


def read_messages(fp):
    for block in iter_blocks(fp):
        blockfp = gzip.GzipFile(fileobj=io.BytesIO(block))
        yield from iter_messages(blockfp)


def read_messages_follow(fp, check_interval=1):
    while True:
        yield from read_messages(fp)
        time.sleep(check_interval)
