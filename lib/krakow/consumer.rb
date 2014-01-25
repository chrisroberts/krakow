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
        debug "Connections will be established via lookup #{nslookupd.inspect}"
        @discovery = Discovery.new(:nslookupd => nslookupd)
        every(60){ init! }
        init!
      elsif(host && port)
        debug "Connection will be established via direct connection #{host}:#{port}"
        connection = build_connection(host, port, queue)
        if(register(connection))
          connections[:default] = connection
        else
          raise Error.new("Failed to establish subscription at provided end point (#{host}:#{port}")
        end
      else
        raise Error.new('No connection information provided!')
      end
    end

    def to_s
      "<#{self.class.name}:#{object_id} T:#{topic} C:#{channel}>"
    end

    def goodbye_my_love!
      debug 'Tearing down consumer'
      connections.values.each do |con|
        con.terminate
      end
      info 'Consumer torn down'
    end

    # host:: remote address
    # port:: remote port
    # queue:: message store queue
    # Build new `Connection`
    def build_connection(host, port, queue)
      connection = Connection.new(
        :host => host,
        :port => port,
        :queue => queue
      )
    end

    # message:: FrameType
    # connection:: Connection
    # Process message if required
    # NOTE: Currently unused
    def process_message(message, connection)
      if(message.is_a?(FrameType::Message))
        connection.transmit(Command::Rdy.new(:count => receive_count || 1))
      end
      message
    end

    # Requests lookup and adds connections
    def init!
      debug 'Running consumer `init!` connection builds'
      found = discovery.lookup(topic)
      debug "Discovery results: #{found.inspect}"
      found.each do |node|
        unless(connections[node[:hostname]])
          connection = build_connection(node[:broadcast_address], node[:tcp_port], queue)
          if(register(connection))
            connections[node[:hostname]] = connection
            info "Registered new connection #{connection}"
          end
        end
      end
    end

    # connection:: Connection
    # Registers connection with subscription. Returns false if failed
    def register(connection)
      connection.init!
      connection.transmit(Command::Sub.new(:topic_name => topic, :channel_name => channel))
      unless(response = connection.queue.pop.is_a?(FrameType::Error))
        connection.transmit(Command::Rdy.new(:count => receive_count || 1))
        true
      else
        error "Failed to establish connection: #{response.error}"
        connection.terminate
        false
      end
    end

    # message_id:: Message ID
    # Confirm message has been processed
    def confirm(message_id)
      writer.transmit(Command::Fin.new(:message_id => message_id))
      writer.transmit(Command::Rdy.new(:count => (receive_count - queue.size)))
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
