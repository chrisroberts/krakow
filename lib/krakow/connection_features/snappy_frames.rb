begin
  require 'snappy'
rescue LoadError
  $stderr.puts 'ERROR: Failed to locate `snappy` gem. Install `snappy` gem into system or bundle.'
  raise
end
require 'digest/crc'
require 'krakow'

module Krakow
  module ConnectionFeatures
    # Snappy functionality
    # @todo Add support for max size + chunks
    # @todo Include support for remaining types
    module SnappyFrames
      # Snappy-able IO
      class Io

        # Header identifier
        IDENTIFIER = "\x73\x4e\x61\x50\x70\x59".force_encoding('ASCII-8BIT')
        ident_size = [IDENTIFIER.size].pack('L<')
        ident_size.slice!(-1,1)
        # Size of identifier
        IDENTIFIER_SIZE = ident_size

        # Mapping of types
        CHUNK_TYPE = {
          "\xff".force_encoding('ASCII-8BIT') => :identifier,
          "\x00".force_encoding('ASCII-8BIT') => :compressed,
          "\x01".force_encoding('ASCII-8BIT') => :uncompressed
        }

        attr_reader :io, :buffer

        # Create new snappy-able IO
        #
        # @param io [IO] IO to wrap
        # @return [Io]
        def initialize(io, args={})
          @io = io
          @snappy_write_ident = false
          @buffer = ''
        end

        # Proxy to underlying socket
        #
        # @param args [Object]
        # @return [Object]
        def method_missing(*args)
          io.__send__(*args)
        end

        # Mask the checksum
        #
        # @param checksum [String]
        # @return [String]
        def checksum_mask(checksum)
          (((checksum >> 15) | (checksum << 17)) + 0xa282ead8) & 0xffffffff
        end

        # Receive bytes from the IO
        #
        # @param n [Integer] nuber of bytes
        # @return [String]
        def recv(n)
          read_stream unless buffer.size >= n
          result = buffer.slice!(0,n)
          result.empty? ? nil : result
        end
        alias_method :read, :recv

        # Read contents from stream
        #
        # @return [String]
        def read_stream
          header = io.recv(4)
          ident = CHUNK_TYPE[header.slice!(0)]
          size = (header << CHUNK_TYPE.key(:compressed)).unpack('L<').first
          content = io.recv(size)
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

        # Write string to IO
        #
        # @param string [String]
        # @return [Integer] number of bytes written
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

        # Send the identifier for snappy content
        #
        # @return [Integer] bytes written
        def send_snappy_identifier
          io.write [CHUNK_TYPE.key(:identifier), IDENTIFIER_SIZE, IDENTIFIER].pack('a*a*a*')
        end

      end
    end
  end
end
