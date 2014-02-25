require 'snappy'
require 'digest/crc'

# TODO: Add support for max size + chunks
# TODO: Include support for remaining types
module SnappyFrames
  class Io

    IDENTIFIER = "\x73\x4e\x61\x50\x70\x59".force_encoding('ASCII-8BIT')
    ident_size = [IDENTIFIER.size].pack('L<')
    ident_size.slice!(-1,1)
    IDENTIFIER_SIZE = ident_size

    CHUNK_TYPE = {
      "\xff".force_encoding('ASCII-8BIT') => :identifier,
      "\x00".force_encoding('ASCII-8BIT') => :compressed,
      "\x01".force_encoding('ASCII-8BIT') => :uncompressed
    }

    attr_reader :io, :buffer

    def initialize(io)
      @io = io
      @snappy_write_ident = false
      @buffer = ''
    end

    # Proxy to underlying socket
    def method_missing(*args)
      io.__send__(*args)
    end

    def checksum_mask(checksum)
      (((checksum >> 15) | (checksum << 17)) + 0xa282ead8) & 0xffffffff
    end

    def recv(n)
      read_stream unless buffer.size >= n
      result = buffer.slice!(0,n)
      result.empty? ? nil : result
    end
    alias_method :read, :recv

    def read_stream
      header = io.read(4)
      ident = CHUNK_TYPE[header.slice!(0)]
      size = (header << CHUNK_TYPE.key(:compressed)).unpack('L<').first
      content = io.read(size)
      case ident
      when :identifier
        unless(content == IDENTIFIER)
          raise "Invalid stream identification encountered (content: #{content.inspect})"
        end
        read_stream
      when :compressed
        checksum = content.slice!(0, 4).unpack('L<').first
        deflated = Snappy.inflate(content)
        digest = Digest::CRC32c.new
        digest << deflated
        unless(checksum == checksum_mask(digest.checksum))
          raise 'Checksum mismatch!'
        end
        buffer << deflated
      when :uncompressed
        buffer << content
      end
    end

    def write(string)
      unless(@snappy_writer_ident)
        send_snappy_identifier
      end
      digest = Digest::CRC32c.new
      digest << string
      content = Snappy.deflate(string)
      size = content.length + 4
      size = [size].pack('L<')
      size.slice!(-1,1)
      checksum = [checksum_mask(digest.checksum)].pack('L<')
      output = [CHUNK_TYPE.key(:compressed), size, checksum, content].pack('a*a*a*a*')
      io.write output
    end

    def send_snappy_identifier
      io.write [CHUNK_TYPE.key(:identifier), IDENTIFIER_SIZE, IDENTIFIER].pack('a*a*a*')
    end

  end
end
