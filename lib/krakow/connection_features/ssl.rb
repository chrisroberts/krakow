module Krakow
  module ConnectionFeatures
    module Ssl
      class Io

        attr_reader :_socket

        def initialize(io, args={})
          ssl_socket_arguments = [io]
          if(true) #args[:ssl_context])
            require 'openssl'
            context = OpenSSL::SSL::SSLContext.new
            context.cert = OpenSSL::X509::Certificate.new(File.open("/home/spox/tls.cert"))
            context.key = OpenSSL::PKey::RSA.new(File.open("/home/spox/tls.key"))
            ssl_socket_arguments << context
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
