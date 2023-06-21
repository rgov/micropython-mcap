import struct
from io import BytesIO


class RecordBuilder:
    def __init__(self):
        self._buffer = BytesIO()

    @property
    def count(self) -> int:
        return self._buffer.tell()

    def start_record(self, opcode: int):
        self._record_start_offset = self._buffer.tell()
        self._buffer.write(struct.pack("<BQ", opcode, 0))  # placeholder size

    def finish_record(self):
        pos = self._buffer.tell()
        length = pos - self._record_start_offset - 9
        self._buffer.seek(self._record_start_offset + 1)
        self._buffer.write(struct.pack("<Q", length))
        self._buffer.seek(pos)

    def end(self):
        buf = self._buffer.getvalue()
        self._buffer.close()
        self._buffer = BytesIO()
        return buf

    def write(self, data: bytes):
        self._buffer.write(data)

    def write_prefixed_string(self, value: str):
        bytes = value.encode()
        self.write4(len(bytes))
        self.write(bytes)

    def write1(self, value: int):
        self.write(struct.pack("<B", value))

    def write2(self, value: int):
        self.write(struct.pack("<H", value))

    def write4(self, value: int):
        self.write(struct.pack("<I", value))

    def write8(self, value: int):
        self.write(struct.pack("<Q", value))
