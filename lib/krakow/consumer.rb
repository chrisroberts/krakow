require 'krakow'

module Krakow
  # Consume messages from a server
  class Consumer

    include Utils::Lazy
    # @!parse include Krakow::Utils::Lazy::InstanceMethods
    # @!parse extend Krakow::Utils::Lazy::ClassMethods

    include Celluloid

    trap_exit :connection_failure
    finalizer :goodbye_my_love!

    attr_reader :connections, :discovery, :distribution, :queue

    # @!group Attributes

    # @!macro [attach] attribute
    #   @!method $1
    #     @return [$2] the $1 $0
    #   @!method $1?
    #     @return [TrueClass, FalseClass] truthiness of the $1 $0
    attribute :topic, String, :required => true
    attribute :channel, String, :required => true
    attribute :host, String
    attribute :port, [String, Integer]
    attribute :nsqlookupd, [Array, String]
    attribute :max_in_flight, Integer, :default => 1
    attribute :backoff_interval, Numeric
    attribute :discovery_interval, Numeric, :default => 30
    attribute :discovery_jitter, Numeric, :default => 10.0
    attribute :notifier, [Celluloid::Signals, Celluloid::Condition, Celluloid::Actor]
    attribute :connection_options, Hash, :default => ->{ Hash.new }

    # @!endgroup

    def initialize(args={})
      super
      arguments[:connection_options] = {:features => {}, :config => {}}.merge(
        arguments[:connection_options] || {}
      )
      @connections = {}
      @distribution = Distribution::Default.new(
        :max_in_flight => max_in_flight,
        :backoff_interval => backoff_interval,
        :consumer => current_actor
      )
      @queue = Queue.new
      if(nsqlookupd)
        debug "Connections will be established via lookup #{nsqlookupd.inspect}"
        @discovery = Discovery.new(:nsqlookupd => nsqlookupd)
        discover
      elsif(host && port)
        debug "Connection will be established via direct connection #{host}:#{port}"
        connection = build_connection(host, port, queue)
        if(register(connection))
          info "Registered new connection #{connection}"
          distribution.redistribute!
        else
          abort Error::ConnectionFailure.new("Failed to establish subscription at provided end point (#{host}:#{port}")
        end
      else
        abort Error::ConfigurationError.new('No connection information provided!')
      end
    end

    # Returns [Krakow::Connection] associated to key
    #
    # @param key [Object] identifier
    # @return [Krakow::Connection] associated connection
    def connection(key)
      @connections[key]
    end

    # @return [String] stringify object
    def to_s
      "<#{self.class.name}:#{object_id} T:#{topic} C:#{channel}>"
    end

    # Instance destructor
    #
    # @return [nil]
    def goodbye_my_love!
      debug 'Tearing down consumer'
      connections.values.each do |con|
        con.terminate if con.alive?
      end
      distribution.terminate if distribution && distribution.alive?
      info 'Consumer torn down'
      nil
    end

    # Build a new [Krakow::Connection]
    #
    # @param host [String] remote host
    # @param port [String, Integer] remote port
    # @param queue [Queue] queue for messages
    # @return [Krakow::Connection, nil] new connection or nil
    def build_connection(host, port, queue)
      begin
        connection = Connection.new(
          :host => host,
          :port => port,
          :queue => queue,
          :topic => topic,
          :channel => channel,
          :notifier => notifier,
          :features => connection_options[:features],
          :features_args => connection_options[:config],
          :callbacks => {
            :handle => {
              :actor => current_actor,
              :method => :process_message
            },
            :reconnect => {
              :actor => current_actor,
              :method => :connection_reconnect
            }
          }
        )
      rescue => e
        error "Failed to build connection (host: #{host} port: #{port} queue: #{queue}) - #{e.class}: #{e}"
        debug "#{e.class}: #{e}\n#{e.backtrace.join("\n")}"
        nil
      end
    end

    # Process a given message if required
    #
    # @param message [Krakow::FrameType]
    # @param connection [Krakow::Connection]
    # @return [Krakow::FrameType]
    def process_message(message, connection)
      if(message.is_a?(FrameType::Message))
        distribution.register_message(message, connection.identifier)
        message.origin = current_actor
      end
      message
    end

    # Action to take when a connection has reconnected
    #
    # @param connection [Krakow::Connection]
    # @return [nil]
    def connection_reconnect(connection)
      connection.transmit(Command::Sub.new(:topic_name => topic, :channel_name => channel))
      distribution.set_ready_for(connection)
      nil
    end

    # Send RDY for connection based on distribution rules
    #
    # @param connection [Krakow::Connection]
    # @return [nil]
    def update_ready!(connection)
      distribution.set_ready_for(connection)
      nil
    end

    # Initialize the consumer by starting lookup and adding connections
    #
    # @return [nil]
    def init!
      debug 'Running consumer `init!` connection builds'
      found = discovery.lookup(topic)
      debug "Discovery results: #{found.inspect}"
      connection = nil
      found.each do |node|
        debug "Processing discovery result: #{node.inspect}"
        key = Connection.identifier(node[:broadcast_address], node[:tcp_port], topic, channel)
        unless(connections[key])
          connection = build_connection(node[:broadcast_address], node[:tcp_port], queue)
          info "Registered new connection #{connection}" if register(connection)
        else
          debug "Discovery result already registered: #{node.inspect}"
        end
      end
      distribution.redistribute! if connection
      nil
    end

    # Start the discovery interval lookup
    #
    # @return [nil]
    def discover
      init!
      after(discovery_interval + (discovery_jitter * rand)){ discover }
    end

    # Register connection with distribution
    #
    # @param connection [Krakow::Connection]
    # @return [TrueClass, FalseClass] true if subscription was successful
    def register(connection)
      begin
        connection.init!
        connection.transmit(Command::Sub.new(:topic_name => topic, :channel_name => channel))
        self.link connection
        connections[connection.identifier] = connection
        distribution.add_connection(connection)
        true
      rescue Error::BadResponse => e
        debug "Failed to establish connection: #{e.result ? e.result.error : '<No Response!>'}"
        connection.terminate
        false
      end
    end

    # Remove connection references when connection is terminated
    #
    # @param actor [Object] terminated actor
    # @param reason [Exception] reason for termination
    # @return [nil]
    def connection_failure(actor, reason)
      connections.delete_if do |key, value|
        if(value == actor && reason.nil?)
          warn "Connection failure detected. Removing connection: #{key} - #{reason || 'no reason provided'}"
          begin
            distribution.remove_connection(key)
          rescue Error::ConnectionUnavailable, Error::ConnectionFailure
            warn 'Caught connection unavailability'
          end
          distribution.redistribute!
          true
        end
      end
      nil
    end

    # Confirm message has been processed
    #
    # @param message_id [String, Krakow::FrameType::Message]
    # @return [TrueClass, FalseClass]
    # @raise [KeyError] connection not found
    def confirm(message_id)
      message_id = message_id.message_id if message_id.respond_to?(:message_id)
      begin
        distribution.in_flight_lookup(message_id) do |connection|
          distribution.unregister_message(message_id)
          connection.transmit(Command::Fin.new(:message_id => message_id))
          distribution.success(connection.identifier)
          update_ready!(connection)
        end
        true
      rescue KeyError => e
        error "Message confirmation failed: #{e}"
        abort e
      rescue Error::LookupFailed => e
        error "Lookup of message for confirmation failed! <Message ID: #{message_id} - Error: #{e}>"
        false
      rescue Error::ConnectionUnavailable => e
        retry
      end
    end
    alias_method :finish, :confirm

    # Requeue message (generally due to processing failure)
    #
    # @param message_id [String, Krakow::FrameType::Message]
    # @param timeout [Numeric]
    # @return [TrueClass]
    def requeue(message_id, timeout=0)
      message_id = message_id.message_id if message_id.respond_to?(:message_id)
      distribution.in_flight_lookup(message_id) do |connection|
        distribution.unregister_message(message_id)
        connection.transmit(
          Command::Req.new(
            :message_id => message_id,
            :timeout => timeout
          )
        )
        distribution.failure(connection.identifier)
        update_ready!(connection)
      end
      true
    end

    # Touch message (to extend timeout)
    #
    # @param message_id [String, Krakow::FrameType::Message]
    # @return [TrueClass]
    def touch(message_id)
      message_id = message_id.message_id if message_id.respond_to?(:message_id)
      distribution.in_flight_lookup(message_id) do |connection|
        connection.transmit(
          Command::Touch.new(:message_id => message_id)
        )
      end
      true
    end

  end
end
