require 'openssl'
require 'krakow'

module Krakow
  module ConnectionFeatures
    # SSL functionality
    module Ssl
      # SSL-able IO
      class Io

        attr_reader :_socket

        # Create new SSL-able IO
        #
        # @param io [IO] IO to wrap
        # @param args [Hash]
        # @option args [Hash] :ssl_context
        # @return [Io]
        def initialize(io, args={})
          ssl_socket_arguments = [io]
          if(args[:ssl_context])
            validate_ssl_args!(args[:ssl_context])
            context = OpenSSL::SSL::SSLContext.new
            context.cert = OpenSSL::X509::Certificate.new(File.open(args[:ssl_context][:certificate]))
            context.key = OpenSSL::PKey::RSA.new(File.open(args[:ssl_context][:key]))
            ssl_socket_arguments << context
          end
          @_socket = OpenSSL::SSL::SSLSocket.new(*ssl_socket_arguments)
          _socket.sync = true
          _socket.connect
        end

        # Proxy to underlying socket
        #
        # @param args [Object]
        # @return [Object]
        def method_missing(*args)
          _socket.send(*args)
        end

        # Receive bytes from the IO
        #
        # @param len [Integer] nuber of bytes
        # @return [String]
        def recv(len)
          str = readpartial(len)
          if(len > str.length)
            str << sysread(len - str.length)
          end
          str
        end

        private

        # Validate the SSL configuration provided
        #
        # @param args [Hash]
        # @option args [String] :certificate path to certificate
        # @option args [String] :key path to key
        # @raise [ArgumentError, LoadError]
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
