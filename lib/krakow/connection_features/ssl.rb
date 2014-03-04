module Krakow
  module ConnectionFeatures
    module Ssl
      class Io

        attr_reader :_socket

        def initialize(io)
          @_socket = io
          _socket.sync = true
          _socket.connect
        end

        def method_missing(*args)
          _socket.send(*args)
        end

        def recv(len)
          str = readpartial(len)
          if(len > str.length)
            str << sysread(len - str.length)
          end
          str
        end

      end
    end
  end
end
