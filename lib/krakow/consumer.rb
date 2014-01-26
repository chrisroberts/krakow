module Krakow
  class Consumer

    include Utils::Lazy
    include Celluloid

    trap_exit :connection_died
    finalizer :goodbye_my_love!

    attr_reader :connections, :discovery, :queue, :in_flight

    def initialize(args={})
      super
      required! :topic, :channel
      optional :host, :port, :nslookupd, :receive_count
      @connections = {}
      @in_flight = {}
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
        con.terminate if con.alive?
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
        :queue => queue,
        :callback => {
          :actor => current_actor,
          :method => :process_message
        }
      )
    end

    # message:: FrameType
    # connection:: Connection
    # Process message if required
    # NOTE: Currently unused
    def process_message(message, connection)
      if(message.is_a?(FrameType::Message))
        in_flight[message.message_id] = "#{connection.host}_#{connection.port}"
      end
      message
    end

    # Requests lookup and adds connections
    def init!
      debug 'Running consumer `init!` connection builds'
      found = discovery.lookup(topic)
      debug "Discovery results: #{found.inspect}"
      found.each do |node|
        debug "Processing discovery result: #{node.inspect}"
        key = "#{node[:broadcast_address]}_#{node[:tcp_port]}"
        unless(connections[key])
          connection = build_connection(node[:broadcast_address], node[:tcp_port], queue)
          if(register(connection))
            connections[key] = connection
            info "Registered new connection #{connection}"
          else
            warn "Failed to register connection #{connection} (#{node.inspect})"
            connection.terminate
          end
        else
          debug "Discovery result already registered: #{node.inspect}"
        end
      end
    end

    # connection:: Connection
    # Registers connection with subscription. Returns false if failed
    def register(connection)
      connection.init!
      connection.transmit(Command::Sub.new(:topic_name => topic, :channel_name => channel))
      begin
        connection.transmit(Command::Rdy.new(:count => receive_count || 1))
        self.link connection
        true
      rescue Error::BadResponse => e
        debug "Failed to establish connection: #{e.result.error}"
        connection.terminate
        false
      end
    end

    def connection_died(con, reason)
      connections.delete_if do |key, value|
        if(value == con)
          warn "Connection failure detected. Removing connection: #{key}"
          true
        end
      end
    end

    # message_id:: Message ID
    # Confirm message has been processed
    def confirm(message_id)
      connection = in_flight_lookup(message_id)
      connection.transmit(Command::Fin.new(:message_id => message_id))
      connection.transmit(Command::Rdy.new(:count => (receive_count - queue.size)))
      true
    end

    # message_id:: Message ID
    # timeout:: Requeue timeout (default is none)
    # Requeue message (processing failure)
    def requeue(message_id, timeout=0)
      in_flight_lookup(message_id).transmit(
        Command::Req.new(:message_id => message_id, :timeout => timeout)
      )
    end

    def in_flight_lookup(msg_id)
      connections[in_flight[msg_id]] ||
        abort(Error.new("Failed to locate connection for in flight message (#{msg_id})"))
    end

  end
end
