require 'celluloid/io'
require 'celluloid/autostart'

module Krakow
  class Connection

    include Utils::Lazy
    include Celluloid::IO

    finalizer :goodbye_my_love!

    attr_reader :socket

    def initialize(args={})
      super
      required! :host, :port
      optional :version, :queue, :callback
      arguments[:queue] ||= Queue.new
      arguments[:version] ||= 'v2'
      @socket = TCPSocket.new(host, port)
    end

    # Initialize the connection
    def init!
      socket.write version.rjust(4).upcase
      async.process_to_queue!
    end

    # message:: Command instance to send
    # Send the message
    def transmit(message)
      socket.write message.to_line
    end

    # Cleanup prior to destruction
    def goodbye_my_love!
      if(socket && !socket.closed?)
        socket.write Command::Cls.new.to_line
        socket.close
      end
      @socket = nil
    end

    # Receive message and return proper FrameType instance
    def receive
      buf = socket.read(8)
      if(buf)
        @receiving = true
        struct = FrameType.decode(buf)
        struct[:data] = socket.read(struct[:size])
        @receiving = false
        FrameType.build(struct)
      else
        nil
      end
    end

    # Currently in the process of receiving a message
    def receiving?
      !!@receiving
    end

    # Pull message and queue
    def process_to_queue!
      loop do
        message = handle(receive)
        if(message)
          queue << message
        end
      end
    end

    # message:: FrameType instance
    # Handle message if not an actual message
    def handle(message)
      # Grab heartbeats upfront
      if(message.is_a?(FrameType::Response) && message.response == '_heartbeat_')
        socket.write Command::Nop.new.to_line
        nil
      else
        if(callback && callback[:actor] && callback[:method])
          callback[:actor].send(callback[:method], message, current_actor)
        else
          message
        end
      end
    end
  end
end
