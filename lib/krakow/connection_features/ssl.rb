module Krakow
  module ConnectionFeatures
    module Ssl
      class Io

        attr_reader :_socket

        def initialize(io, args={})
          ssl_socket_arguments = [io]
          if(args[:ssl_context])
            # ssl_socket_arguments << SSLContext.new
          end
          @_socket = Celluloid::IO::SSLSocket.new(*ssl_socket_arguments)
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
