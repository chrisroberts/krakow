module Krakow
  module ConnectionFeatures
    module Ssl
      class Io

        attr_reader :_socket

        def initialize(io, args={})
          ssl_socket_arguments = [io]
          if(args[:ssl_context])
            validate_ssl_args!(args[:ssl_context])
            require 'openssl'
            context = OpenSSL::SSL::SSLContext.new
            context.cert = OpenSSL::X509::Certificate.new(File.open(args[:ssl_context][:certificate]))
            context.key = OpenSSL::PKey::RSA.new(File.open(args[:ssl_context][:key]))
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

        private

        def validate_ssl_args!(args)
          [:key, :certificate].each do |arg_key|
            unless(args.has_key?(arg_key))
              raise ArgumentError.new "The `:ssl_context` option requires `#{arg_key.inspect}` to be set"
            end
            unless(File.readable?(args[arg_key]))
              raise LoadError.new "Unable to read the `#{arg_key.inspect}` file from the `:ssl_context` arguments"
            end
          end
        end

      end
    end
  end
end
