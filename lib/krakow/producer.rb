require 'krakow'

module Krakow

  # TCP based producer
  class Producer

    autoload :Http, 'krakow/producer/http'

    include Utils::Lazy
    # @!parse include Utils::Lazy::InstanceMethods
    # @!parse extend Utils::Lazy::ClassMethods

    include Celluloid

    trap_exit  :connection_failure
    finalizer :producer_cleanup

    # set exclusive methods
    exclusive :write

    attr_reader :connection
    attr_reader :notifier

    # @!group Attributes

    # @!macro [attach] attribute
    #   @!method $1
    #     @return [$2] the $1 $0
    #   @!method $1?
    #     @return [TrueClass, FalseClass] truthiness of the $1 $0
    attribute :host, String, :required => true
    attribute :port, [String, Integer], :required => true
    attribute :topic, String, :required => true
    attribute :reconnect_retries, Integer, :default => 10
    attribute :reconnect_interval, Integer, :default => 5
    attribute :connection_options, Hash, :default => ->{ Hash.new }

    # @!endgroup

    def initialize(args={})
      super
      arguments[:connection_options] = {:features => {}, :config => {}, :options => {}}.merge(
        arguments.fetch(:connection_options, {})
      )
      connect
    end

    # Establish connection to configured `host` and `port`
    #
    # @return nil
    def connect
      @connecting = true
      info "Establishing connection to: #{host}:#{port}"
      begin
        con_args = connection_options[:options].dup.tap do |args|
          args[:host] = host
          args[:port] = port
          if(connection_options[:features])
            args[:features] = connection_options[:features]
          end
          if(connection_options[:config])
            args[:features_args] = connection_options[:config]
          end
        end
        @connection = Connection.new(con_args)
        @connection.init!
        self.link @connection
        info "Connection established: #{@connection}"
        nil
      rescue => e
        abort e
      end
      @connecting = false
    end

    # @return [String] stringify object
    def to_s
      "<#{self.class.name}:#{object_id} {#{host}:#{port}} T:#{topic}>"
    end

    # @return [TrueClass, FalseClass] currently connected to server
    def connected?
      begin
        !!(!@connecting &&
          connection &&
          connection.alive? &&
          connection.connected?)
      rescue Celluloid::DeadActorError
        false
      end
    end

    # Process connection failure and attempt reconnection
    #
    # @return [TrueClass]
    def connection_failure(obj, reason)
      if(obj == connection && !reason.nil?)
        begin
          @connection = nil
          warn "Connection failure detected for #{host}:#{port} - #{reason}"
          obj.terminate if obj.alive?
          connect
        rescue => reason
          warn "Failed to establish connection to #{host}:#{port}. Pausing #{reconnect_interval} before retry"
          sleep reconnect_interval
          retry
        end
      end
      true
    end

    # Instance destructor
    # @return nil
    def producer_cleanup
      debug 'Tearing down producer'
      if(connection && connection.alive?)
        connection.terminate
      end
      @connection = nil
      info 'Producer torn down'
      nil
    end

    # Write message to server
    #
    # @param message [String] message to write
    # @return [Krakow::FrameType, TrueClass]
    # @note if connection response wait is set to 0, writes will
    #   return a `true` value on completion
    # @raise [Krakow::Error::ConnectionUnavailable]
    def write(*message)
      if(message.empty?)
        abort ArgumentError.new 'Expecting one or more messages to send. None provided.'
      end
      begin
        if(message.size > 1)
          debug 'Multiple message publish'
          connection.transmit(
            Command::Mpub.new(
              :topic_name => topic,
              :messages => message
            )
          )
        else
          debug 'Single message publish'
          connection.transmit(
            Command::Pub.new(
              :message => message.first,
              :topic_name => topic
            )
          )
        end
      rescue Celluloid::Task::TerminatedError
        abort Error::ConnectionUnavailable.new 'Connection is currently unavailable'
      rescue => e
        abort e
      end
    end

  end
end
