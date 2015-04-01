require 'zlib'
require 'krakow'

module Krakow
  module ConnectionFeatures
    # Deflate functionality
    module Deflate
      # Deflatable IO
      class Io

        attr_reader :io, :buffer, :headers, :inflator, :deflator

        # Create new deflatable IO
        #
        # @param io [IO] IO to wrap
        # @return [Io]
        def initialize(io, args={})
          @io = io.raw_socket
          @buffer = ''
          @inflator = Zlib::Inflate.new(-Zlib::MAX_WBITS)
          @deflator = Zlib::Deflate.new(nil, -Zlib::MAX_WBITS)
          io.socket = self
        end

        # Proxy to underlying socket
        #
        # @param args [Object]
        # @return [Object]
        def method_missing(*args)
          io.__send__(*args)
        end

        # Receive bytes from the IO
        #
        # @param n [Integer] nuber of bytes
        # @return [String]
        def recv(n)
          until(buffer.length >= n)
            read_stream
            sleep(0.1) unless buffer.length >= n
          end
          buffer.slice!(0, n)
        end
        alias_method :read, :recv

        # Read contents from stream
        #
        # @return [String]
        def read_stream
          str = io.read
          unless(str.empty?)
            buffer << inflator.inflate(str)
          end
        end

        # Write string to IO
        #
        # @param string [String]
        # @return [Integer] number of bytes written
        def write(string)
          unless(string.empty?)
            output = deflator.deflate(string)
            output << deflator.flush
            io.write(output)
          else
            0
          end
        end

        # Close the IO
        #
        # @return [TrueClass]
        def close(*args)
          super
          deflator.deflate(nil, Zlib::FINISH)
          deflator.close
          true
        end

      end
    end
  end
end
