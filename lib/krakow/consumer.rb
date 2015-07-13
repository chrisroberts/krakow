require 'krakow'

module Krakow
  # Consume messages from a server
  class Consumer

    autoload :Queue, 'krakow/consumer/queue'

    include Utils::Lazy
    # @!parse include Krakow::Utils::Lazy::InstanceMethods
    # @!parse extend Krakow::Utils::Lazy::ClassMethods

    include Zoidberg::Supervise

#    trap_exit :connection_failure

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
    attribute :notifier, [Zoidberg::Signal]
    attribute :connection_options, Hash, :default => ->{ Hash.new }

    # @!endgroup

    def initialize(args={})
      super
      arguments[:connection_options] = {:features => {}, :config => {}}.merge(
        arguments[:connection_options] || {}
      )
      @connections = {}
      @queue = Queue.new(
        current_self,
        :removal_callback => :remove_message
      )
      @distribution = Distribution::Default.new(
        :max_in_flight => max_in_flight,
        :backoff_interval => backoff_interval,
        :consumer => current_self
      )
      if(nsqlookupd)
        debug "Connections will be established via lookup #{nsqlookupd.inspect}"
        @discovery = Discovery.new(:nsqlookupd => nsqlookupd)
        discover
      elsif(host && port)
        direct_connect
      else
        abort Error::ConfigurationError.new('No connection information provided!')
      end
    end

    # @return [TrueClass, FalseClass] currently connected to at least
    #   one nsqd
    def connected?
      !!connections.values.any? do |con|
        con.connected?
      end
    end

    # Connect to nsqd instance directly
    #
    # @return [Connection]
    def direct_connect
      debug "Connection will be established via direct connection #{host}:#{port}"
      connection = build_connection(host, port, queue)
      if(register(connection))
        info "Registered new connection #{connection}"
        distribution.redistribute!
      else
        abort Error::ConnectionFailure.new("Failed to establish subscription at provided end point (#{host}:#{port}")
      end
      connection
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
    def terminate(*_)
      debug 'Tearing down consumer'
      if(distribution && distribution.alive?)
        distribution.terminate
      end
      if(queue && queue.alive?)
        queue.terminate
      end
      connections.values.each do |con|
        con.terminate if con.alive?
      end
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
              :actor => current_self,
              :method => :process_message
            }
          }
        )
        queue.register_connection(connection)
        connection
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
    # @note If we receive a message that is already in flight, attempt
    #   to scrub message from wait queue. If message is found, retry
    #   distribution registration. If message is not found, assume it
    #   is currently being processed and do not allow new message to
    #   be queued
    def process_message(message, connection)
      discard = false
      if(message.is_a?(FrameType::Message))
        message.origin = current_self
        message.connection = connection
        retried = false
        unregister = false
        begin
          distribution.register_message(message, connection.identifier)
        rescue KeyError => e
          if(!retried && queue.scrub_duplicate_message(message))
            warn "Received duplicate message. Attempting to scrub from currently queued messages. (#{message})"
            retried = true
            retry
          elsif(!unregister)
            warn "Received message is currently in flight and not in wait queue. Removing from in flight! (#{message})"
            distribution.unregister_message(message)
            unregister = true
            retry
          else
            error "Received message cannot be added into distribution do to key conflict (#{message})"
            discard = true
          end
        end
      end
      discard ? nil : message
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
      if(reason && key = connections.key(actor))
        warn "Connection failure detected. Removing connection: #{key} - #{reason}"
        connections.delete(key)
        begin
          distribution.remove_connection(key)
        rescue Error::ConnectionUnavailable, Error::ConnectionFailure
          warn 'Caught connection unavailability'
        end
        queue.deregister_connection(key)
        distribution.redistribute!
        direct_connect unless discovery
      end
      nil
    end

    # Remove message
    #
    # @param messages [Array<FrameType::Message>]
    # @return [NilClass]
    # @note used mainly for queue callback
    def remove_message(messages)
      [messages].flatten.compact.each do |msg|
        distribution.unregister_message(msg.message_id)
        update_ready!(msg.connection)
      end
      nil
    end

    # Confirm message has been processed
    #
    # @param message_id [String, Krakow::FrameType::Message]
    # @return [TrueClass]
    # @raise [KeyError] connection not found
    def confirm(message_id)
      message_id = message_id.message_id if message_id.respond_to?(:message_id)
      begin
        begin
          connection = distribution.in_flight_lookup(message_id)
          connection.transmit(Command::Fin.new(:message_id => message_id))
          distribution.success(connection.identifier)
        rescue => e
          abort e
        end
        true
      rescue KeyError => e
        error "Message confirmation failed: #{e}"
        abort e
      rescue Error::LookupFailed => e
        error "Lookup of message for confirmation failed! <Message ID: #{message_id} - Error: #{e}>"
        abort e
      rescue Error::ConnectionUnavailable => e
        abort e
      ensure
        con = distribution.unregister_message(message_id)
        update_ready!(con) if con
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
      begin
        distribution.in_flight_lookup(message_id) do |connection|
          connection.transmit(
            Command::Touch.new(:message_id => message_id)
          )
        end
        true
      rescue Error::LookupFailed => e
        error "Lookup of message for touch failed! <Message ID: #{message_id} - Error: #{e}>"
        abort e
      end
    end

  end
end
