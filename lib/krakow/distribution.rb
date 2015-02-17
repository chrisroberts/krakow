require 'krakow'

module Krakow
  # Message distribution
  # @abstract
  class Distribution

    autoload :Default, 'krakow/distribution/default'
#    autoload :ProducerWeighted, 'krakow/distribution/producer_weighted'
#    autoload :ConsumerWeighted, 'krakow/distribution/consumer_weighted'

    include Celluloid
    include Utils::Lazy
    # @!parse include Krakow::Utils::Lazy::InstanceMethods
    # @!parse extend Krakow::Utils::Lazy::ClassMethods

    attr_accessor :ideal, :flight_record, :registry

    # @!group Attributes

    # @!macro [attach] attribute
    #   @!method $1
    #     @return [$2] the $1 $0
    #   @!method $1?
    #     @return [TrueClass, FalseClass] truthiness of the $1 $0
    attribute :consumer, Krakow::Consumer, :required => true
    attribute :watch_dog_interval, Numeric, :default => 1.0
    attribute :backoff_interval, Numeric
    attribute :max_in_flight, Integer, :default => 1

    # @!endgroup

    def initialize(args={})
      super
      @ideal = 0
      @flight_record = {}
      @registry = {}
    end

    # [Abstract] Reset flight distributions
    def redistribute!
      raise NotImplementedError.new 'Custom `#redistrubute!` method must be provided!'
    end

    # [Abstract] Determine RDY value for given connection
    # @param connection_identifier [String]
    # @return [Integer]
    def calculate_ready!(connection_identifier)
      raise NotImplementedError.new 'Custom `#calculate_ready!` method must be provided!'
    end

    # Remove message metadata from registry
    #
    # @param message [Krakow::FrameType::Message, String] message or ID
    # @return [Krakow::Connection]
    def unregister_message(message)
      msg_id = message.respond_to?(:message_id) ? message.message_id : message.to_s
      connection = connection_lookup(flight_record[msg_id])
      registry_info = registry_lookup(connection.identifier)
      flight_record.delete(msg_id)
      registry_info[:in_flight] -= 1
      calculate_ready!(connection.identifier)
      connection
    end

    # Return the currently configured RDY value for given connnection
    #
    # @param connection_identifier [String]
    # @return [Integer]
    def ready_for(connection_identifier)
      registry_lookup(connection_identifier)[:ready]
    end


    # Send RDY for given connection
    #
    # @param connection [Krakow::Connection]
    # @return [Krakow::FrameType::Error,nil]
    def set_ready_for(connection, *_)
      connection.transmit(
        Command::Rdy.new(
          :count => ready_for(connection.identifier)
        )
      )
    end

    # Initial ready value used for new connections
    #
    # @return [Integer]
    def initial_ready
      ideal > 0 ? 1 : 0
    end

    # Registers message into registry and configures for distribution
    #
    # @param message [FrameType::Message]
    # @param connection_identifier [String]
    # @return [Integer]
    def register_message(message, connection_identifier)
      if(flight_record[message.message_id])
        abort KeyError.new "Message is already registered in flight record! (#{message.message_id})"
      else
        registry_info = registry_lookup(connection_identifier)
        registry_info[:in_flight] += 1
        flight_record[message.message_id] = connection_identifier
        calculate_ready!(connection_identifier)
      end
    end

    # Add connection to make available for RDY distribution
    #
    # @param connection [Krakow::Connection]
    # @return [TrueClass]
    def add_connection(connection)
      unless(registry[connection.identifier])
        registry[connection.identifier] = {
          :ready => initial_ready,
          :in_flight => 0,
          :failures => 0,
          :backoff_until => 0
        }
      end
      true
    end

    # Remove connection from RDY distribution
    #
    # @param connection_identifier [String]
    # @return [TrueClass]
    def remove_connection(connection_identifier, *args)
      # remove connection from registry
      registry.delete(connection_identifier)
      # remove any in flight messages
      flight_record.delete_if do |k,v|
        if(v == connection_identifier)
          warn "Removing in flight reference due to failed connection: #{v}"
          true
        end
      end
      true
    end

    # Return connection associated with given registry key
    #
    # @param identifier [String] connection identifier
    # @return [Krakow::Connection, nil]
    def connection_lookup(identifier)
      consumer.connection(identifier)
    end

    # Return source connection for given message ID
    #
    # @param msg_id [String]
    # @yield execute with connection
    # @yieldparam connection [Krakow::Connection]
    # @return [Krakow::Connection, Object]
    def in_flight_lookup(msg_id)
      connection = connection_lookup(flight_record[msg_id])
      unless(connection)
        abort Krakow::Error::LookupFailed.new("Failed to locate in flight message (ID: #{msg_id})")
      end
      if(block_given?)
        begin
          yield connection
        rescue => e
          abort e
        end
      else
        connection
      end
    end

    # Return registry information for given connection
    # @param connection_identifier [String]
    # @return [Hash] registry information
    # @raise [Krakow::Error::LookupFailed]
    def registry_lookup(connection_identifier)
      registry[connection_identifier] ||
        abort(Krakow::Error::LookupFailed.new("Failed to locate connection information in registry (#{connection_identifier})"))
    end

    # @return [Array<Krakow::Connection>] connections in registry
    def connections
      registry.keys.map do |identifier|
        connection_lookup(identifier)
      end.compact
    end

    # Log failure of processed message
    #
    # @param connection_identifier [String]
    # @return [TrueClass]
    def failure(connection_identifier)
      if(backoff_interval)
        registry_info = registry_lookup(connection_identifier)
        registry_info[:failures] += 1
        registry_info[:backoff_until] = Time.now.to_i + (registry_info[:failures] * backoff_interval)
      end
      true
    end

    # Log success of processed message
    #
    # @param connection_identifier [String]
    # @return [TrueClass]
    def success(connection_identifier)
      if(backoff_interval)
        registry_info = registry_lookup(connection_identifier)
        if(registry_info[:failures] > 1)
          registry_info[:failures] -= 1
          registry_info[:backoff_until] = Time.now.to_i + (registry_info[:failures] * backoff_interval)
        else
          registry_info[:failures] = 0
        end
      end
      true
    end

  end
end
