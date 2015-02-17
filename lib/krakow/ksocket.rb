require 'krakow'
require 'socket'

module Krakow
  class Ksocket

    include Utils::Lazy
    include Celluloid

    # @return [String]
    attr_reader :buffer

    finalizer :closedown_socket

    def closedown_socket
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
        @socket = args[:socket]
      else
        unless([:host, :port].all?{|k| args.include?(k)})
          raise ArgumentError.new 'Missing required arguments. Expecting `:socket` or `:host` and `:port`.'
        end
        @socket = TCPSocket.new(args[:host], args[:port])
      end
      @buffer = ''
      async.read_loop
    end

    # @return [TrueClass, FalseClass] read loop enabled
    def reading?
      !!@reading
    end

    # Read from socket and push into local Queue
    def read_loop
      unless(reading?)
        @reading = true
        while(reading?)
          res = defer do
            Kernel.select([socket], nil, nil, nil)
            socket{|s| s.readpartial(1024)}
          end
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

    def socket
      if(block_given?)
        yield @socket
      else
        @socket
      end
    end

  end
end
