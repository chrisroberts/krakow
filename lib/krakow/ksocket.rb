require 'krakow'
require 'socket'
require 'fiber'

module Krakow
  class Ksocket

    include Utils::Lazy
    include Zoidberg::Shell

    # @return [String]
    attr_reader :buffer
    # @return [TCPSocket]
    attr_reader :raw_socket

    # Teardown helper
    def terminate(error=nil)
      debug "Tearing down ksocket (Error: #{error.class} - #{error})"
      @writing = @reading = false
      if(socket && !socket.closed?)
        socket.close
      end
    end

    # Create new socket wrapper
    #
    # @param args [Hash]
    # @option args [Socket-ish] :socket
    # @option args [String] :host
    # @option args [Integer] :port
    # @return [self]
    def initialize(args={})
      if(args[:socket])
        @raw_socket = args[:socket]
      else
        unless([:host, :port].all?{|k| args.include?(k)})
          raise ArgumentError.new 'Missing required arguments. Expecting `:socket` or `:host` and `:port`.'
        end
        @make_socket = lambda{TCPSocket.new(args[:host], args[:port])}
        @raw_socket = TCPSocket.new(args[:host], args[:port])
      end
      @buffer = ''
    end

    # @return [TrueClass, FalseClass] read loop enabled
    def reading?
      !!@reading
    end

    # Replace socket instance with new socket
    #
    # @param sock [Socket]
    # @return [Socket]
    def socket=(sock)
      socket{|s| @raw_socket = sock }
      sock
    end

    # Read from socket and push into local Queue
    def read_loop
      unless(reading?)
        @reading = true
        while(reading?)
          Kernel.select([@raw_socket], nil, nil, nil)
          res = @raw_socket.readpartial(1024)
          if(res)
            debug "Received content from socket: #{res.inspect}"
            buffer << res
            signal(:content_read)
          else
            debug 'No content received from socket read. Ignoring.'
          end
        end
      end
    end

    # Fetch bytes from socket
    #
    # @param n [Integer]
    # @return [String]
    def get(n)
      until(buffer.length >= n)
        wait(:content_read)
      end
      buffer.slice!(0, n)
    end
    alias_method :recv, :get
    alias_method :read, :get
    alias_method :sysread, :get
    alias_method :readpartial, :get

    # Push bytes to socket
    #
    # @param line [String]
    # @return [Integer]
    def put(line)
      socket{|s| s.write(line)}
    end
    alias_method :write, :put

    # @return [Socket]
    def socket
      if(block_given?)
        yield @raw_socket
      else
        @raw_socket
      end
    end

  end
end
