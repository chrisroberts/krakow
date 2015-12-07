require 'krakow'

module Krakow

  # TCP based producer
  class Producer

    autoload :Http, 'krakow/producer/http'

    include Utils::Lazy
    # @!parse include Utils::Lazy::InstanceMethods
    # @!parse extend Utils::Lazy::ClassMethods

    include Zoidberg::SoftShell
    include Zoidberg::Supervise

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
      !!(!@connecting &&
        connection &&
        connection.alive? &&
        connection.connected?)
    end

    # Instance destructor
    # @return nil
    def terminate(error=nil)
      debug "Tearing down producer (Error: #{error.class} - #{error})"
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
      rescue Zoidberg::DeadException
        raise Error::ConnectionUnavailable.new
      end
    end

  end
end
