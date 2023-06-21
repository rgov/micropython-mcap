from typing import Dict, List, Tuple
import zlib

from .data_stream import RecordBuilder
from .opcode import Opcode


class McapRecord:
    def write(self, stream: RecordBuilder) -> None:
        raise NotImplementedError()


class Attachment(McapRecord):
    def __init__(self, create_time: int, log_time: int, name: str,
                 media_type: str, data: bytes):
        self.create_time = create_time
        self.log_time = log_time
        self.name = name
        self.media_type = media_type
        self.data = data

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


class AttachmentIndex(McapRecord):
    def __init__(self, offset: int, length: int, log_time: int,
                 create_time: int, data_size: int, name: str, media_type: str):
        self.offset = offset
        self.length = length
        self.log_time = log_time
        self.create_time = create_time
        self.data_size = data_size
        self.name = name
        self.media_type = media_type

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


class Channel(McapRecord):
    def __init__(self, id: int, topic: str, message_encoding: str,
                 metadata: Dict[str, str], schema_id: int):
        self.id = id
        self.topic = topic
        self.message_encoding = message_encoding
        self.metadata = metadata
        self.schema_id = schema_id

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


class Chunk(McapRecord):
    def __init__(self, compression: str, data: bytes, message_end_time: int,
                message_start_time: int, uncompressed_crc: int,
                uncompressed_size: int):
        self.compression = compression
        self.data = data
        self.message_end_time = message_end_time
        self.message_start_time = message_start_time
        self.uncompressed_crc = uncompressed_crc
        self.uncompressed_size = uncompressed_size

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


class ChunkIndex(McapRecord):
    def __init__(self, chunk_length: int, chunk_start_offset: int,
                 compression: str, compressed_size: int,
                 message_end_time: int, message_index_length: int,
                 message_index_offsets: Dict[int, int],
                 message_start_time: int, uncompressed_size: int):
        self.chunk_length = chunk_length
        self.chunk_start_offset = chunk_start_offset
        self.compression = compression
        self.compressed_size = compressed_size
        self.message_end_time = message_end_time
        self.message_index_length = message_index_length
        self.message_index_offsets = message_index_offsets
        self.message_start_time = message_start_time
        self.uncompressed_size = uncompressed_size

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


class DataEnd(McapRecord):
    def __init__(self, data_section_crc: int):
        self.data_section_crc = data_section_crc

    def write(self, stream: RecordBuilder):
        stream.start_record(Opcode.DATA_END)
        stream.write4(self.data_section_crc)
        stream.finish_record()


class Footer(McapRecord):
    def __init__(self, summary_start: int, summary_offset_start: int,
                 summary_crc: int):
        self.summary_start = summary_start
        self.summary_offset_start = summary_offset_start
        self.summary_crc = summary_crc

    def write(self, stream: RecordBuilder):
        stream.start_record(Opcode.FOOTER)
        stream.write8(self.summary_start)
        stream.write8(self.summary_offset_start)
        stream.write4(self.summary_crc)
        stream.finish_record()


class Header(McapRecord):
    def __init__(self, profile: str, library: str):
        self.profile = profile
        self.library = library

    def write(self, stream: RecordBuilder):
        stream.start_record(Opcode.HEADER)
        stream.write_prefixed_string(self.profile)
        stream.write_prefixed_string(self.library)
        stream.finish_record()


class Message(McapRecord):
    def __init__(self, channel_id: int, log_time: int, data: bytes,
                 publish_time: int, sequence: int):
        self.channel_id = channel_id
        self.log_time = log_time
        self.data = data
        self.publish_time = publish_time
        self.sequence = sequence

    def write(self, stream: RecordBuilder):
        stream.start_record(Opcode.MESSAGE)
        stream.write2(self.channel_id)
        stream.write4(self.sequence)
        stream.write8(self.log_time)
        stream.write8(self.publish_time)
        stream.write(self.data)
        stream.finish_record()


class MessageIndex(McapRecord):
    def __init__(self, channel_id: int, records: List[Tuple[int, int]]):
        self.channel_id = channel_id
        self.records = records

    def write(self, stream: RecordBuilder):
        stream.start_record(Opcode.MESSAGE_INDEX)
        stream.write2(self.channel_id)
        stream.write4(len(self.records) * 16)
        for timestamp, offset in self.records:
            stream.write8(timestamp)
            stream.write8(offset)
        stream.finish_record()


class Metadata(McapRecord):
    def __init__(self, name: str, metadata: Dict[str, str]):
        self.name = name
        self.metadata = metadata

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


class MetadataIndex(McapRecord):
    def __init__(self, offset: int, length: int, name: str):
        self.offset = offset
        self.length = length
        self.name = name

    def write(self, stream: RecordBuilder) -> None:
        stream.start_record(Opcode.METADATA_INDEX)
        stream.write8(self.offset)
        stream.write8(self.length)
        stream.write_prefixed_string(self.name)
        stream.finish_record()


class Schema(McapRecord):
    def __init__(self, id: int, data: bytes, encoding: str, name: str):
        self.id = id
        self.data = data
        self.encoding = encoding
        self.name = name

    def write(self, stream: RecordBuilder):
        stream.start_record(Opcode.SCHEMA)
        stream.write2(self.id)
        stream.write_prefixed_string(self.name)
        stream.write_prefixed_string(self.encoding)
        stream.write4(len(self.data))
        stream.write(self.data)
        stream.finish_record()


class Statistics(McapRecord):
    def __init__(self, attachment_count: int, channel_count: int,
                 channel_message_counts: Dict[int, int], chunk_count: int,
                 message_count: int, message_end_time: int,
                 message_start_time: int, metadata_count: int,
                 schema_count: int):
        self.attachment_count = attachment_count
        self.channel_count = channel_count
        self.channel_message_counts = channel_message_counts
        self.chunk_count = chunk_count
        self.message_count = message_count
        self.message_end_time = message_end_time
        self.message_start_time = message_start_time
        self.metadata_count = metadata_count
        self.schema_count = schema_count

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


class SummaryOffset(McapRecord):
    def __init__(self, group_opcode: int, group_start: int, group_length: int):
        self.group_opcode = group_opcode
        self.group_start = group_start
        self.group_length = group_length

    def write(self, stream: RecordBuilder):
        stream.start_record(Opcode.SUMMARY_OFFSET)
        stream.write1(self.group_opcode)
        stream.write8(self.group_start)
        stream.write8(self.group_length)
        stream.finish_record()
