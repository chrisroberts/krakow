module Krakow
  class Distribution

    autoload :Default, 'krakow/distribution/default'
#    autoload :ProducerWeighted, 'krakow/distribution/producer_weighted'
#    autoload :ConsumerWeighted, 'krakow/distribution/consumer_weighted'

    include Celluloid
    include Utils::Lazy

    attr_accessor :max_in_flight, :ideal, :flight_record, :registry

    def initialize(args={})
      super
      optional :watch_dog_interval, :backoff_interval
      arguments[:watch_dog_interval] ||= 5
      if(arguments[:backoff_interval].nil?)
        arguments[:backoff_interval] = 1
      end
      @max_in_flight = arguments[:max_in_flight] || 1
      @ideal = 0
      @flight_record = {}
      @registry = {}
    end

    # Reset flight distributions
    def redistribute!
      raise NoMethodError.new 'Custom `#redistrubute!` method must be provided!'
    end

    # connection:: Connection
    # Determine RDY value for given connection
    def calculate_ready!(connection)
      raise NoMethodError.new 'Custom `#calculate_ready!` method must be provided!'
    end

    # message:: FrameType::Message or message ID string
    # Remove message metadata from registry. Should be used after
    # confirmations or requeue.
    def unregister_message(message)
      msg_id = message.respond_to?(:message_id) ? message.message_id : message.to_s
      connection = flight_record[msg_id]
      # TODO: Add lookup error
      registry_info = registry[connection_key(connection)]
      flight_record.delete(msg_id)
      registry_info[:in_flight] -= 1
      calculate_ready!(connection)
      connection
    end

    # connection:: Connection
    # Return the currently configured RDY value for given connnection
    def ready_for(connection)
      registry_lookup(connection)[:ready]
    end

    # connection:: Connection
    # Send RDY for given connection
    def set_ready_for(connection, *_)
      connection.transmit(
        Command::Rdy.new(
          :count => ready_for(connection)
        )
      )
    end

    # Initial ready value used for new connections
    def initial_ready
      ideal > 0 ? 1 : 0
    end

    # message:: FrameType::Message
    # connection:: Connection
    # Registers message into registry and configures for distribution
    def register_message(message, connection)
      registry_info = registry_lookup(connection)
      registry_info[:in_flight] += 1
      flight_record[message.message_id] = connection_key(connection)
      calculate_ready!(connection)
    end

    # connection:: Connection
    # Add connection to make available for RDY distribution
    def add_connection(connection)
      registry[connection_key(connection)] = {
        :ready => initial_ready,
        :in_flight => 0,
        :failures => 0
      }
      true
    end

    # connection:: Connection
    # Remove connection from RDY distribution
    def remove_connection(connection)
      # remove connection from registry
      registry.delete(connection_key(connection))
      # remove any in flight messages
      in_flight.delete_if do |k,v|
        v == connection_key(connection)
      end
      true
    end

    # connection:: Connection
    # Return lookup key (actor reference)
    def connection_key(connection)
      connection.current_actor
    end

    # msg_id:: Message ID string
    # Return source connection of given `msg_id`. If block is
    # provided, the connection instance will be yielded to the block
    # and the result returned.
    def in_flight_lookup(msg_id)
      connection = flight_record[msg_id]
      unless(connection)
        abort LookupFailed.new("Failed to locate in flight message (ID: #{msg_id})")
      end
      if(block_given?)
        yield connection
      else
        connection
      end
    end

    # connection:: Connection
    # Return registry information for given connection
    def registry_lookup(connection)
      registry[connection_key(connection)] ||
        abort(LookupFailed.new("Failed to locate connection information in registry (#{connection})"))
    end

    # Return list of all connections in registry
    def connections
      registry.keys
    end

    # connection:: Connection
    # Log failure of processed message
    def error(connection)
      if(backoff_interval)
        registry_info = registry_lookup(connection)
        registry_info[:failures] += 1
        registry_info[:backoff_until] = Time.now.to_i + (registry_info[:failures] * backoff_interval)
      end
      true
    end

    # connection:: Connection
    # Log success of processed message
    def success(connection)
      if(backoff_interval)
        registry_info = registry_lookup(connection)
        if(registry_info[:failures] > 1)
          registry_info[:failures] -= 1
          registry_info[:backoff_until] = Time.now.to_i + (registry_info[:failures] * backoff_interval)
        else
          registry_info[:failures] = 0
        end
      end
    end

  end
end
