require 'krakow'
require 'socket'
require 'fiber'

module Krakow
  class Ksocket

    include Utils::Lazy
    include Zoidberg::SoftShell

    # @return [String]
    attr_reader :buffer
    # @return [Queue]
    attr_reader :incoming
    # @return [TCPSocket]
    attr_reader :raw_socket

    def terminate
      begin
        @raw_socket.close
      rescue
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
      @incoming = Queue.new
      if(args[:socket])
        @raw_socket = args[:socket]
      else
        unless([:host, :port].all?{|k| args.include?(k)})
          raise ArgumentError.new 'Missing required arguments. Expecting `:socket` or `:host` and `:port`.'
        end
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
      @raw_socket = sock
    end

    def socket
      @raw_socket
    end

    def put(string)
      idx = 0
      until(idx == string.length)
        result = raw_socket.write_nonblock(
          string[0, string.length - idx],
          :exception => false
        )
        if(result == :wait_writable)
          defer{ IO.select(nil, [raw_socket]) }
        else
          idx += result
        end
      end
      idx
    end
    alias_method :write, :put

    def get(length)
      begin
        raw_socket.read_nonblock(length)
      rescue IO::WaitReadable
        defer{ IO.select([raw_socket], nil) }
        retry
      end
    end
    alias_method :recv, :get
    alias_method :read, :get
    alias_method :sysread, :get
    alias_method :readpartial, :get

  end
end
