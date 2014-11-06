# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'openssl'

module Avro
  # Support for writing and reading encoded and compressed Avro data to and
  # from Object Container Files.
  #
  # @see http://avro.apache.org/docs/current/spec.html#Object+Container+Files Object Container File specification
  module DataFile
    # Format version of the Avro Object Container File specification.
    VERSION = 1

    # Magic bytes indicating format version, written to file headers.
    MAGIC = "Obj" + [VERSION].pack('c')
    MAGIC.force_encoding('BINARY') if MAGIC.respond_to?(:force_encoding)

    # Size in bytes of the format version magic indicator.
    MAGIC_SIZE = MAGIC.respond_to?(:bytesize) ? MAGIC.bytesize : MAGIC.size

    # Schema of metadata component in file headers, according to specification,
    # which must include the schema of objects stored in the file, and may
    # contain arbitrary user-specified metadata.
    META_SCHEMA = Schema.parse('{"type": "map", "values": "bytes"}')

    # Size in bytes of sync markers demarcating each data block in a file.
    SYNC_SIZE = 16

    # Interval in bytes after which a synchronization marker is written if the
    # size of a data block exceeds it.
    #
    # @see https://cwiki.apache.org/confluence/display/AVRO/FAQ#FAQ-Whatisthepurposeofthesyncmarkerintheobjectfileformat? What is the purpose of the sync marker in the object file format?
    SYNC_INTERVAL = 4000 * SYNC_SIZE

    # Not yet used.
    VALID_ENCODINGS = ['binary']

    class DataFileError < AvroError; end

    # Open an encoded Avro data file for reading or writing.
    #
    # If a block is given, the yielded {Writer} or {Reader} will have its
    # underlying file handle closed automatically when the block terminates.
    #
    # @param file_path [String, Pathname]
    #   Path of file to open.
    # @param mode ['r', 'w']
    #   Whether to open file for reading (+r+) or writing (+w+).
    # @param schema [String]
    #   The schema to use for reading or writing serialized objects. When in
    #   read mode, the given schema will be considered the "reader's schema"
    #   for schema resoluton purposes.
    # @param codec
    #   The codec to use when writing. If write mode is specified and no codec
    #   is given, appending to an existing file is assumed and the file must
    #   already contain metadata declaring a codec.
    #
    # @yield [Reader, Writer]
    #   A {Reader} or {Writer} to a given block, depending on mode specified.
    #
    # @return [Reader, Writer]
    #   A {Reader} or {Writer}, depending on the mode specified.
    #
    # @raise [DataFileError]
    #   if any value other than 'r' or 'w' is given for +mode+ param, or write
    #   mode is specified but no schema is given.
    #
    # @see http://avro.apache.org/docs/current/spec.html#Schema+Resolution Schema Resolution
    #
    # @todo Accept an Avro::Schema instance for schema param
    def self.open(file_path, mode='r', schema=nil, codec=nil)
      schema = Avro::Schema.parse(schema) if schema
      case mode
      when 'w'
        # TODO: Writer allows giving no schema and tries to append using existing
        # schema; we should probably be consistent and keep or drop that behavior,
        # codec is handled similarly, and otherwise it seems this method provides
        # no means of appending; update docs here for the decisions
        #
        # TODO: Inconsistent semantics for write mode with schema param being
        # required but codec allowed to be nil--it looks like this will break,
        # write a test
        unless schema
          raise DataFileError, "Writing an Avro file requires a schema."
        end
        io = open_writer(File.open(file_path, 'wb'), schema, codec)
      when 'r'
        io = open_reader(File.open(file_path, 'rb'), schema)
      else
        raise DataFileError, "Only modes 'r' and 'w' allowed. You gave #{mode.inspect}."
      end

      yield io if block_given?
      io
    ensure
      io.close if block_given? && io
    end

    # Registered compression codecs for Avro data.
    #
    # @return [Hash]
    #   Mapping of codecs, where keys are String codec names and values are
    #   classes implementing the codec interface.
    def self.codecs
      @codecs
    end

    # Register a compression codec for Avro data.
    #
    # @param codec
    #   A class or instance implementing the codec interface.
    #
    # @return The codec instance that was registered.
    #
    # @todo
    #   There ought to be a formal abstract class or mixin for the codec interface.
    def self.register_codec(codec)
      @codecs ||= {}
      codec = codec.new if !codec.respond_to?(:codec_name) && codec.is_a?(Class)
      @codecs[codec.codec_name.to_s] = codec
    end

    # Convenience method to look up a codec instance, given any of several types
    # of codec identifier.
    #
    # @param codec
    #   A codec class, instance of a codec class, or the String name of a
    #   registered codec.
    #
    # @return A codec instance
    def self.get_codec(codec)
      codec ||= 'null'
      if codec.respond_to?(:compress) && codec.respond_to?(:decompress)
        codec # it's a codec instance
      elsif codec.is_a?(Class)
        codec.new # it's a codec class
      elsif @codecs.include?(codec.to_s)
        @codecs[codec.to_s] # it's a string or symbol (codec name)
      else
        raise DataFileError, "Unknown codec: #{codec.inspect}"
      end
    end

    class << self
      private
      def open_writer(file, schema, codec=nil)
        writer = IO::DatumWriter.new(schema)
        Writer.new(file, writer, schema, codec)
      end

      def open_reader(file, schema)
        reader = IO::DatumReader.new(nil, schema)
        Reader.new(file, reader)
      end
    end

    # A Writer instance manages the details of writing a valid Avro Container
    # Object File while presenting a simple abstraction to users, akin to
    # adding Ruby data structures conforming to the schema (objects) to an
    # Array (the container file).
    #
    # It is typically most convenient to use a Writer yielded by {DataFile.open}
    # rather than instantiating directly.
    #
    # @todo At present, giving no writer's schema upon instantation implicitly
    #   invokes append behavior. This is not completely intuitive and there
    #   should probably be an explicit append mode.
    class Writer
      def self.generate_sync_marker
        OpenSSL::Random.random_bytes(16)
      end

      # TODO: most of these can/should be private
      attr_reader :writer, :encoder, :datum_writer, :buffer_writer, :buffer_encoder, :sync_marker, :meta, :codec
      attr_accessor :block_count

      # Instantiate a new Writer for a given Avro file.
      #
      # @param writer [File]
      #   The file to write.
      # @param datum_writer [DatumWriter]
      #   A DatumWriter instance. Its schema will be set automatically depending
      #   on whether the Writer will create a new file or append.
      # @param writers_schema [Schema, String]
      #   Schema to write in the header of a new file, as a {Schema} instance
      #   or in String form. If not given, it is assumed the Writer will
      #   append to an existing file.
      # @param codec
      #   A lookup identifier for which {get_codec} can return a codec instance.
      #
      # @return [Writer]
      #
      # @todo To avoid the implicit append assumption, callers must redundantly
      #   give the writer's schema as a direct param even though they might
      #   instead have set it on the DatumWriter instance. This is weird.
      #   There's really no reason for a caller to need to pass a DatumWriter.
      def initialize(writer, datum_writer, writers_schema=nil, codec=nil)
        @writer = writer
        @encoder = IO::BinaryEncoder.new(@writer)
        @datum_writer = datum_writer
        @buffer_writer = StringIO.new('', 'w')
        @buffer_writer.set_encoding('BINARY') if @buffer_writer.respond_to?(:set_encoding)
        @buffer_encoder = IO::BinaryEncoder.new(@buffer_writer)
        @block_count = 0

        @meta = {}

        # If writers_schema is not present, presume we're appending
        #
        # TODO: This has flaws. It should be possible to explicitly append even
        # when a schema is given, then schema resolution or bailing should be
        # considered if new objects are being written but schema is different
        # from the file's metadata.
        if writers_schema
          @sync_marker = Writer.generate_sync_marker
          @codec = DataFile.get_codec(codec)
          meta['avro.codec'] = @codec.codec_name.to_s
          meta['avro.schema'] = writers_schema.to_s
          datum_writer.writers_schema = writers_schema
          write_header
        else
          # open writer for reading to collect metadata
          #
          # TODO: Need to throw an exception if the file does not exist and is
          # not an Avro file with existing schema.
          dfr = Reader.new(writer, Avro::IO::DatumReader.new)

          # FIXME(jmhodges): collect arbitrary metadata
          # collect metadata
          @sync_marker = dfr.sync_marker
          meta['avro.codec'] = dfr.meta['avro.codec']
          @codec = DataFile.get_codec(meta['avro.codec'])

          # get schema used to write existing file
          schema_from_file = dfr.meta['avro.schema']
          meta['avro.schema'] = schema_from_file
          datum_writer.writers_schema = Schema.parse(schema_from_file)

          # seek to the end of the file and prepare for writing
          writer.seek(0,2)
        end
      end

      # Append a datum to the file.
      #
      # If the block size threshold has been reached, a synchronization marker
      # will be written.
      #
      # @param datum [Object]
      #   A Ruby object conforming to the {Schema} of this Writer instance.
      def <<(datum)
        datum_writer.write(datum, buffer_encoder)
        self.block_count += 1

        # if the data to write is larger than the sync interval, write
        # the block
        if buffer_writer.tell >= SYNC_INTERVAL
          write_block
        end
      end

      # Return the current position as a value that may be passed to
      # Reader#reader.seek. Forces the end of the current block, emitting a
      # synchronization marker.
      def sync
        write_block
        writer.tell
      end

      # Flush the current state of the file, including metadata
      def flush
        write_block
        writer.flush
      end

      # Close the file, flushing any pending buffered block.
      def close
        flush
        writer.close
      end

      private

      def write_header
        # write magic
        writer.write(MAGIC)

        # write metadata
        datum_writer.write_data(META_SCHEMA, meta, encoder)

        # write sync marker
        writer.write(sync_marker)
      end

      # TODO(jmhodges): make a schema for blocks and use datum_writer
      # TODO(jmhodges): do we really need the number of items in the block?
      def write_block
        if block_count > 0
          # write number of items in block and block size in bytes
          encoder.write_long(block_count)
          to_write = codec.compress(buffer_writer.string)
          encoder.write_long(to_write.respond_to?(:bytesize) ? to_write.bytesize : to_write.size)

          # write block contents
          writer.write(to_write)

          # write sync marker
          writer.write(sync_marker)

          # reset buffer
          buffer_writer.truncate(0)
          buffer_writer.rewind
          self.block_count = 0
        end
      end
    end

    # A Reader instance manages the details of reading the Object Container File
    # format, in order to present the objects contained in a file to users as an
    # {Enumerable} collection.
    #
    # It is typically most convenient to use a Reader yielded by {DataFile.open}
    # rather than instantiating directly.
    class Reader
      include ::Enumerable

      # The reader and binary decoder for the raw file stream
      attr_reader :reader, :decoder

      # The binary decoder for the contents of a block (after codec decompression)
      attr_reader :block_decoder

      attr_reader :datum_reader, :sync_marker, :meta, :file_length, :codec
      attr_accessor :block_count # records remaining in current block

      # Instantiate a new Reader for a given Avro file.
      #
      # @param reader [File]
      #   The file from which to read.
      # @param datum_reader [DatumReader]
      #   A DatumReader initialized with a reader's schema for the file's
      #   objects. The writer's schema will be set from the file metadata.
      #
      # @return [Reader]
      def initialize(reader, datum_reader)
        @reader = reader
        @decoder = IO::BinaryDecoder.new(reader)
        @datum_reader = datum_reader

        # read the header: magic, meta, sync
        read_header

        @codec = DataFile.get_codec(meta['avro.codec'])

        # get ready to read
        @block_count = 0
        datum_reader.writers_schema = Schema.parse meta['avro.schema']
      end

      # Iterates through each datum in this file
      #
      # @yield [Object]
      #   The deserialized object for each datum.
      # @return [void]
      #
      # @todo Handle block of length zero
      # @todo Return Enumerator when no block given
      def each
        loop do
          if block_count == 0
            case
            when eof? then break
            when skip_sync
              break if eof?
              read_block_header
            else
              read_block_header
            end
          end

          datum = datum_reader.read(block_decoder)
          self.block_count -= 1
          yield(datum)
        end
      end

      # Whether the Reader has reached the end of the container file.
      #
      # @return [Boolean]
      def eof?; reader.eof?; end

      # Close the handle to the Reader's underlying file.
      #
      # @return [void]
      def close
        reader.close
      end

      private
      def read_header
        # seek to the beginning of the file to get magic block
        reader.seek(0, 0)

        # check magic number
        magic_in_file = reader.read(MAGIC_SIZE)
        if magic_in_file.size < MAGIC_SIZE
          msg = 'Not an Avro data file: shorter than the Avro magic block'
          raise DataFileError, msg
        elsif magic_in_file != MAGIC
          msg = "Not an Avro data file: #{magic_in_file.inspect} doesn't match #{MAGIC.inspect}"
          raise DataFileError, msg
        end

        # read metadata
        @meta = datum_reader.read_data(META_SCHEMA,
                                       META_SCHEMA,
                                       decoder)
        # read sync marker
        @sync_marker = reader.read(SYNC_SIZE)
      end

      def read_block_header
        self.block_count = decoder.read_long
        block_bytes = decoder.read_long
        data = codec.decompress(reader.read(block_bytes))
        @block_decoder = IO::BinaryDecoder.new(StringIO.new(data))
      end

      # read the length of the sync marker; if it matches the sync
      # marker, return true. Otherwise, seek back to where we started
      # and return false
      def skip_sync
        proposed_sync_marker = reader.read(SYNC_SIZE)
        if proposed_sync_marker != sync_marker
          reader.seek(-SYNC_SIZE, 1)
          false
        else
          true
        end
      end
    end


    # Null codec implementation, passing data through with no compression.
    class NullCodec
      def codec_name; 'null'; end
      def decompress(data); data; end
      def compress(data); data; end
    end

    # Deflate codec implementation, supporting serialized object data with RFC
    # 1951 zlib compression.
    class DeflateCodec
      attr_reader :level

      def initialize(level=Zlib::DEFAULT_COMPRESSION)
        @level = level
      end

      def codec_name; 'deflate'; end

      def decompress(compressed)
        # Passing a negative number to Inflate puts it into "raw" RFC1951 mode
        # (without the RFC1950 header & checksum). See the docs for
        # inflateInit2 in http://www.zlib.net/manual.html
        zstream = Zlib::Inflate.new(-Zlib::MAX_WBITS)
        data = zstream.inflate(compressed)
        data << zstream.finish
      ensure
        zstream.close
      end

      def compress(data)
        zstream = Zlib::Deflate.new(level, -Zlib::MAX_WBITS)
        compressed = zstream.deflate(data)
        compressed << zstream.finish
      ensure
        zstream.close
      end
    end

    DataFile.register_codec NullCodec
    DataFile.register_codec DeflateCodec

    # The default registered compression codecs.
    #
    # @deprecated Use {DataFile.codecs}--this constant won't be updated if you
    #   register additional codecs.
    VALID_CODECS = DataFile.codecs.keys
  end
end

