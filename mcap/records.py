from dataclasses import dataclass, field
from typing import Dict, List, Tuple
import zlib

from .data_stream import RecordBuilder
from .opcode import Opcode


@dataclass
class McapRecord:
    def write(self, stream: RecordBuilder) -> None:
        raise NotImplementedError()


@dataclass
class Attachment(McapRecord):
    create_time: int
    log_time: int
    name: str
    media_type: str
    data: bytes

    def write(self, stream: RecordBuilder):
        builder = RecordBuilder()
        builder.start_record(Opcode.ATTACHMENT)
        builder.write8(self.log_time)
        builder.write8(self.create_time)
        builder.write_prefixed_string(self.name)
        builder.write_prefixed_string(self.media_type)
        builder.write8(len(self.data))
        builder.write(self.data)
        builder.write4(0)  # crc
        builder.finish_record()
        data = memoryview(builder.end())
        stream.write(data[:-4])
        stream.write4(zlib.crc32(data[9:-4]))


@dataclass
class AttachmentIndex(McapRecord):
    offset: int
    length: int
    log_time: int
    create_time: int
    data_size: int
    name: str
    media_type: str

    def write(self, stream: RecordBuilder):
        stream.start_record(Opcode.ATTACHMENT_INDEX)
        stream.write8(self.offset)
        stream.write8(self.length)
        stream.write8(self.log_time)
        stream.write8(self.create_time)
        stream.write8(self.data_size)
        stream.write_prefixed_string(self.name)
        stream.write_prefixed_string(self.media_type)
        stream.finish_record()


@dataclass
class Channel(McapRecord):
    id: int
    topic: str
    message_encoding: str
    metadata: Dict[str, str]
    schema_id: int

    def write(self, stream: RecordBuilder):
        stream.start_record(Opcode.CHANNEL)
        stream.write2(self.id)
        stream.write2(self.schema_id)
        stream.write_prefixed_string(self.topic)
        stream.write_prefixed_string(self.message_encoding)
        meta_length = 0
        for k, v in self.metadata.items():
            meta_length += 8
            meta_length += len(k.encode())
            meta_length += len(v.encode())
        stream.write4(meta_length)
        for k, v in self.metadata.items():
            stream.write_prefixed_string(k)
            stream.write_prefixed_string(v)
        stream.finish_record()


@dataclass
class Chunk(McapRecord):
    compression: str
    data: bytes = field(repr=False)
    message_end_time: int
    message_start_time: int
    uncompressed_crc: int
    uncompressed_size: int

    def write(self, stream: RecordBuilder):
        stream.start_record(Opcode.CHUNK)
        stream.write8(self.message_start_time)
        stream.write8(self.message_end_time)
        stream.write8(self.uncompressed_size)
        stream.write4(self.uncompressed_crc)
        stream.write_prefixed_string(self.compression)
        stream.write8(len(self.data))
        stream.write(self.data)
        stream.finish_record()


@dataclass
class ChunkIndex(McapRecord):
    chunk_length: int
    chunk_start_offset: int
    compression: str
    compressed_size: int
    message_end_time: int
    message_index_length: int
    message_index_offsets: Dict[int, int]
    message_start_time: int
    uncompressed_size: int

    def write(self, stream: RecordBuilder):
        stream.start_record(Opcode.CHUNK_INDEX)
        stream.write8(self.message_start_time)
        stream.write8(self.message_end_time)
        stream.write8(self.chunk_start_offset)
        stream.write8(self.chunk_length)
        stream.write4(len(self.message_index_offsets) * 10)
        for id, offset in self.message_index_offsets.items():
            stream.write2(id)
            stream.write8(offset)
        stream.write8(self.message_index_length)
        stream.write_prefixed_string(self.compression)
        stream.write8(self.compressed_size)
        stream.write8(self.uncompressed_size)
        stream.finish_record()


@dataclass
class DataEnd(McapRecord):
    data_section_crc: int

    def write(self, stream: RecordBuilder):
        stream.start_record(Opcode.DATA_END)
        stream.write4(self.data_section_crc)
        stream.finish_record()


@dataclass
class Footer(McapRecord):
    summary_start: int
    summary_offset_start: int
    summary_crc: int

    def write(self, stream: RecordBuilder):
        stream.start_record(Opcode.FOOTER)
        stream.write8(self.summary_start)
        stream.write8(self.summary_offset_start)
        stream.write4(self.summary_crc)
        stream.finish_record()


@dataclass
class Header(McapRecord):
    profile: str
    library: str

    def write(self, stream: RecordBuilder):
        stream.start_record(Opcode.HEADER)
        stream.write_prefixed_string(self.profile)
        stream.write_prefixed_string(self.library)
        stream.finish_record()


@dataclass
class Message(McapRecord):
    channel_id: int
    log_time: int
    data: bytes
    publish_time: int
    sequence: int

    def write(self, stream: RecordBuilder):
        stream.start_record(Opcode.MESSAGE)
        stream.write2(self.channel_id)
        stream.write4(self.sequence)
        stream.write8(self.log_time)
        stream.write8(self.publish_time)
        stream.write(self.data)
        stream.finish_record()


@dataclass
class MessageIndex(McapRecord):
    channel_id: int
    records: List[Tuple[int, int]]

    def write(self, stream: RecordBuilder):
        stream.start_record(Opcode.MESSAGE_INDEX)
        stream.write2(self.channel_id)
        stream.write4(len(self.records) * 16)
        for timestamp, offset in self.records:
            stream.write8(timestamp)
            stream.write8(offset)
        stream.finish_record()


@dataclass
class Metadata(McapRecord):
    name: str
    metadata: Dict[str, str]

    def write(self, stream: RecordBuilder) -> None:
        stream.start_record(Opcode.METADATA)
        stream.write_prefixed_string(self.name)
        meta_length = 0
        for k, v in self.metadata.items():
            meta_length += 8
            meta_length += len(k.encode())
            meta_length += len(v.encode())
        stream.write4(meta_length)
        for k, v in self.metadata.items():
            stream.write_prefixed_string(k)
            stream.write_prefixed_string(v)
        stream.finish_record()


@dataclass
class MetadataIndex(McapRecord):
    offset: int
    length: int
    name: str

    def write(self, stream: RecordBuilder) -> None:
        stream.start_record(Opcode.METADATA_INDEX)
        stream.write8(self.offset)
        stream.write8(self.length)
        stream.write_prefixed_string(self.name)
        stream.finish_record()


@dataclass
class Schema(McapRecord):
    id: int
    data: bytes
    encoding: str
    name: str

    def write(self, stream: RecordBuilder):
        stream.start_record(Opcode.SCHEMA)
        stream.write2(self.id)
        stream.write_prefixed_string(self.name)
        stream.write_prefixed_string(self.encoding)
        stream.write4(len(self.data))
        stream.write(self.data)
        stream.finish_record()


@dataclass
class Statistics(McapRecord):
    attachment_count: int
    channel_count: int
    channel_message_counts: Dict[int, int]
    chunk_count: int
    message_count: int
    message_end_time: int
    message_start_time: int
    metadata_count: int
    schema_count: int

    def write(self, stream: RecordBuilder):
        stream.start_record(Opcode.STATISTICS)
        stream.write8(self.message_count)
        stream.write2(self.schema_count)
        stream.write4(self.channel_count)
        stream.write4(self.attachment_count)
        stream.write4(self.metadata_count)
        stream.write4(self.chunk_count)
        stream.write8(self.message_start_time)
        stream.write8(self.message_end_time)
        stream.write4(len(self.channel_message_counts) * 10)
        for id, count in self.channel_message_counts.items():
            stream.write2(id)
            stream.write8(count)
        stream.finish_record()


@dataclass
class SummaryOffset(McapRecord):
    group_opcode: int
    group_start: int
    group_length: int

    def write(self, stream: RecordBuilder):
        stream.start_record(Opcode.SUMMARY_OFFSET)
        stream.write1(self.group_opcode)
        stream.write8(self.group_start)
        stream.write8(self.group_length)
        stream.finish_record()
