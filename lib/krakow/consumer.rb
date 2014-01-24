module Krakow
  class Consumer

    include Utils::Lazy
    include Celluloid

    finalizer :goodbye_my_love!

    attr_reader :connections, :discovery, :queue

    def initialize(args={})
      super
      required! :topic, :channel
      optional :host, :port, :nslookupd, :receive_count
      @connections = {}
      @queue = Queue.new
      if(nslookupd)
        @discovery = Discovery.new(:nslookupd => nslookupd)
        every(60){ init! }
        init!
      else
        connection = build_connection(host, port, queue)
        if(register(connection))
          connections[:default] = connection
        else
          raise Error.new("Failed to establish subscription at provided end point (#{host}:#{port}")
        end
      end
    end

    def goodbye_my_love!
      connections.values.each do |con|
        con.terminate
      end
    end

    def build_connection(host, port, queue)
      connection = Connection.new(
        :host => host,
        :port => port,
        :queue => queue
      )
    end

    def process_message(message, connection)
      puts 'PROCESSING!'
      if(message.is_a?(FrameType::Message))
        connection.transmit(Command::Rdy.new(:count => receive_count || 1))
      end
      message
    end

    # Requests lookup and adds connections
    def init!
      found = discovery.lookup(topic)
      found.each do |node|
        unless(connections[node[:hostname]])
          connection = build_connection(node[:broadcast_address], node[:tcp_port], queue)
          connections[node[:hostname]] = connection if register(connection)
        end
      end
    end

    # connection:: Connection
    # Registers connection with subscription. Returns false if failed
    def register(connection)
      connection.init!
      connection.transmit(Command::Sub.new(:topic_name => topic, :channel_name => channel))
      unless(connection.queue.pop.is_a?(FrameType::Error))
        connection.transmit(Command::Rdy.new(:count => receive_count || 1))
        true
      else
        connection.terminate
        false
      end
    end

    # message_id:: Message ID
    # Confirm message has been processed
    def confirm(message_id)
      writer.transmit(Command::Fin.new(:message_id => message_id))
      writer.transmit(Command::Rdy.new(:count => (receive_count - queue.size) + 1))
      true
    end

    # message_id:: Message ID
    # timeout:: Requeue timeout (default is none)
    # Requeue message (processing failure)
    def requeue(message_id, timeout=0)
      writer.transmit(Command::Req.new(:message_id => message_id, :timeout => timeout))
    end

    # Attempt to return free connection from pool for writing
    def writer
      connections.values.detect do |con|
        !con.receiving?
      end || connections.values.first
    end

  end
end
