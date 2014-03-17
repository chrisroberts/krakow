require 'zlib'

module Krakow
  module ConnectionFeatures
    module Deflate
      class Io

        attr_reader :io, :buffer, :headers, :inflator, :deflator

        def initialize(io, args={})
          @io = io
          @buffer = ''
          @inflator = Zlib::Inflate.new(-Zlib::MAX_WBITS)
          @deflator = Zlib::Deflate.new(nil, -Zlib::MAX_WBITS)
        end

        # Proxy to underlying socket
        def method_missing(*args)
          io.__send__(*args)
        end

        def recv(n)
          until(buffer.length >= n)
            read_stream
            sleep(0.1) unless buffer.length >= n
          end
          buffer.slice!(0, n)
        end
        alias_method :read, :recv

        def read_stream
          str = io.read
          unless(str.empty?)
            buffer << inflator.inflate(str)
          end
        end

        def write(string)
          unless(string.empty?)
            output = deflator.deflate(string)
            output << deflator.flush
            io.write(output)
          else
            0
          end
        end

        def close(*args)
          super
          deflator.deflate(nil, Zlib::FINISH)
          deflator.close
        end

      end
    end
  end
end
