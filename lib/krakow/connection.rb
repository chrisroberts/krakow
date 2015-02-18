require 'krakow'

module Krakow

  # Provides TCP connection to NSQD
  class Connection

    # Generate identifier for connection
    #
    # @param host [String]
    # @param port [String, Integer]
    # @param topic [String]
    # @param channel [String]
    # @return [String]
    def self.identifier(host, port, topic, channel)
      [host, port, topic, channel].compact.join('__')
    end

    include Utils::Lazy
    # @!parse include Krakow::Utils::Lazy::InstanceMethods
    # @!parse extend Krakow::Utils::Lazy::ClassMethods

    include Celluloid

    # Available connection features
    FEATURES = [
      :max_rdy_count,
      :max_msg_timeout,
      :msg_timeout,
      :tls_v1,
      :deflate,
      :deflate_level,
      :max_deflate_level,
      :snappy,
      :sample_rate,
      :auth_required
    ]

    # List of features that may not be enabled together
    EXCLUSIVE_FEATURES = [[:snappy, :deflate]]

    # List of features that may be enabled by the client
    ENABLEABLE_FEATURES = [:tls_v1, :snappy, :deflate, :auth_required]

    finalizer :connection_cleanup

    # @return [Hash] current configuration for endpoint
    attr_reader :endpoint_settings
    # @return [Ksocket] underlying socket like instance
    attr_reader :socket
    # @return [TrueClass, FalseClass]
    attr_reader :running

    # @!group Attributes

    # @!macro [attach] attribute
    #   @!method $1
    #     @return [$2] the $1 $0
    #   @!method $1?
    #     @return [TrueClass, FalseClass] truthiness of the $1 $0
    attribute :host, String, :required => true
    attribute :port, [String,Integer], :required => true
    attribute :topic, String
    attribute :channel, String
    attribute :version, String, :default => 'v2'
    attribute :queue, [Queue, Consumer::Queue], :default => ->{ Queue.new }
    attribute :callbacks, Hash, :default => ->{ Hash.new }
    attribute :responses, Queue, :default => ->{ Queue.new }
    attribute :notifier, [Celluloid::Signals, Celluloid::Condition, Celluloid::Actor]
    attribute :features, Hash, :default => ->{ Hash.new }
    attribute :response_wait, Numeric, :default => 1.0
    attribute :response_interval, Numeric, :default => 0.03
    attribute :error_wait, Numeric, :default => 0
    attribute :enforce_features, [TrueClass,FalseClass], :default => true
    attribute :features_args, Hash, :default => ->{ Hash.new }

    # @!endgroup

    # Create new instance
    #
    # @param args [Hash]
    # @option args [String] :host (required) server host
    # @option args [String, Numeric] :port (required) server port
    # @option args [String] :version
    # @option args [Queue] :queue received message queue
    # @option args [Hash] :callbacks
    # @option args [Queue] :responses received responses queue
    # @option args [Celluloid::Actor] :notifier actor to notify on new message
    # @option args [Hash] :features features to enable
    # @option args [Numeric] :response_wait time to wait for response
    # @option args [Numeric] :response_interval sleep interval for wait loop
    # @option args [Numeric] :error_wait time to wait for error response
    # @option args [TrueClass, FalseClass] :enforce_features fail if features are unavailable
    # @option args [Hash] :feature_args options for connection features
    def initialize(args={})
      super
      @endpoint_settings = {}
      @running = false
    end

    # @return [String] identifier for this connection
    def identifier
      self.class.identifier(host, port, topic, channel)
    end

    # @return [String] stringify object
    def to_s
      "<#{self.class.name}:#{object_id} {#{host}:#{port}}>"
    end

    # Initialize the connection
    #
    # @return [nil]
    def init!
      connect!
      async.process_to_queue!
      nil
    end

    # Send message to remote server
    #
    # @param message [Krakow::Message] message to send
    # @return [TrueClass, Krakow::FrameType] response if expected or true
    def transmit(message)
      unless(message.respond_to?(:to_line))
        abort TypeError.new("Expecting type `Krakow::FrameType` but received `#{message.class}`")
      end
      output = message.to_line
      response_wait = wait_time_for(message)
      if(response_wait > 0)
        transmit_with_response(message, response_wait)
      else
        debug ">>> #{output}"
        socket.put(output)
        true
      end
    end

    # Sends message and waits for response
    #
    # @param message [Krakow::Message] message to send
    # @return [Krakow::FrameType] response
    def transmit_with_response(message, wait_time)
      responses.clear
      socket.put(message.to_line)
      response = nil
      (wait_time / response_interval).to_i.times do |i|
        response = responses.pop unless responses.empty?
        break if response
        sleep(response_interval)
      end
      if(response)
        message.response = response
        if(message.error?(response))
          res = Error::BadResponse.new "Message transmission failed #{message}"
          res.result = response
          abort res
        end
        response
      else
        unless(Command.response_for(message) == :error_only)
          abort Error::BadResponse::NoResponse.new "No response provided for message #{message}"
        end
      end
    end

    # Destructor method for cleanup
    #
    # @return [nil]
    def connection_cleanup
      debug 'Tearing down connection'
      @running = false
      if(connected?)
        socket.terminate
      end
      @socket = nil
      info 'Connection torn down'
      nil
    end

    # Receive from server
    #
    # @return [Krakow::FrameType, nil] message or nothing if read was empty
    # @raise [Error::ConnectionUnavailable] socket is closed
    def receive
      debug 'Read wait for frame start'
      buf = socket.get(8)
      if(buf)
        @receiving = true
        debug "<<< #{buf.inspect}"
        struct = FrameType.decode(buf)
        debug "Decoded structure: #{struct.inspect}"
        struct[:data] = socket.get(struct[:size])
        debug "<<< #{struct[:data].inspect}"
        @receiving = false
        frame = FrameType.build(struct)
        debug "Struct: #{struct.inspect} Frame: #{frame.inspect}"
        frame
      else
        nil
      end
    end

    # @return [TrueClass, FalseClass] is connection currently receiving a message
    def receiving?
      !!@receiving
    end

    # Receive messages and place into queue
    #
    # @return [nil]
    def process_to_queue!
      unless(@running)
        @running = true
        while(@running)
          message = handle(receive)
          if(message)
            debug "Adding message to queue #{message}"
            queue << message
            if(notifier)
              warn "Sending new message notification: #{notifier} - #{message}"
              notifier.broadcast(message)
            end
          else
            debug 'Received `nil` message. Ignoring.'
          end
        end
      end
      nil
    end

    # Handle non-message type Krakow::FrameType
    #
    # @param message [Krakow::FrameType] received message
    # @return [Krakow::FrameType, nil]
    def handle(message)
      # Grab heartbeats upfront
      if(message.is_a?(FrameType::Response) && message.response == '_heartbeat_')
        debug 'Responding to heartbeat'
        transmit Command::Nop.new
        nil
      else
        message = callback_for(:handle, message)
        if(!message.is_a?(FrameType::Message))
          debug "Captured non-message type response: #{message}"
          responses << message
          nil
        else
          message
        end
      end
    end

    # Execute callback for given type
    #
    # @overload callback_for(type, arg, connection)
    #   @param type [Symbol] type of callback
    #   @param arg [Object] argument for callback (can be multiple)
    #   @param connection [Krakow::Connection] current connection
    # @return [Object] result of callback
    def callback_for(type, *args)
      callback = callbacks[type]
      if(callback)
        debug "Processing connection callback for #{type.inspect} (#{callback.inspect})"
        if(callback[:actor].alive?)
          callback[:actor].send(callback[:method], *(args + [current_actor]))
        else
          error "Expected actor for callback processing is not alive! (type: `#{type.inspect}`)"
        end
      else
        debug "No connection callback defined for #{type.inspect}"
        args.size == 1 ? args.first : args
      end
    end

    # Returns configured wait time for given message type
    #
    # @param message [Krakow::Command]
    # @return [Numeric] seconds to wait
    def wait_time_for(message)
      case Command.response_for(message)
      when :required
        response_wait
      when :error_only
        error_wait
      else
        0
      end
    end

    # @return [Hash] default settings for IDENTIFY
    def identify_defaults
      unless(@identify_defaults)
        @identify_defaults = {
          :short_id => Socket.gethostname,
          :long_id => Socket.gethostbyname(Socket.gethostname).flatten.compact.first,
          :user_agent => "krakow/#{Krakow::VERSION}",
          :feature_negotiation => true
        }
      end
      @identify_defaults
    end

    # IDENTIFY with server and negotiate features
    #
    # @return [TrueClass]
    def identify_and_negotiate
      expected_features = identify_defaults.merge(features)
      ident = Command::Identify.new(
        expected_features
      )
      socket.put(ident.to_line)
      response = receive
      if(expected_features[:feature_negotiation])
        begin
          @endpoint_settings = MultiJson.load(response.content, :symbolize_keys => true)
          info "Connection settings: #{endpoint_settings.inspect}"
          # Enable things we need to enable
          ENABLEABLE_FEATURES.each do |key|
            if(endpoint_settings[key])
              send(key)
            elsif(enforce_features && expected_features[key])
              abort Error::ConnectionFeatureFailure.new("Failed to enable #{key} feature on connection!")
            end
          end
        rescue MultiJson::LoadError => e
          error "Failed to parse response from Identify request: #{e} - #{response}"
          abort e
        end
      else
        @endpoint_settings = {}
      end
      true
    end

    # Send authentication request for connection
    #
    # @return [TrueClass]
    def auth_required
      info 'Authentication required for this connection'
      if(feature_args[:auth])
        transmit(Command::Auth.new(:secret => feature_args[:auth]))
        response = receive
        true
      else
        error 'No authentication information provided for connection!'
        abort 'Authentication failure. No authentication secret provided'
      end
    end

    # Enable snappy feature on underlying socket
    #
    # @return [TrueClass]
    def snappy
      info 'Loading support for snappy compression and converting connection'
      @socket = ConnectionFeatures::SnappyFrames::Io.new(socket, features_args)
      response = receive
      info "Snappy connection conversion complete. Response: #{response.inspect}"
      true
    end

    # Enable deflate feature on underlying socket
    #
    # @return [TrueClass]
    def deflate
      debug 'Loading support for deflate compression and converting connection'
      @socket = ConnectionFeatures::Deflate::Io.new(socket, features_args)
      response = receive
      info "Deflate connection conversion complete. Response: #{response.inspect}"
      true
    end

    # Enable TLS feature on underlying socket
    #
    # @return [TrueClass]
    def tls_v1
      info 'Enabling TLS for connection'
      @socket = ConnectionFeatures::Ssl::Io.new(socket, features_args)
      response = receive
      info "TLS enable complete. Response: #{response.inspect}"
      true
    end

    # @return [TrueClass, FalseClass] underlying socket is connected
    def connected?
      begin
        !!(socket && socket.alive?)
      rescue Celluloid::DeadActorError
        false
      end
    end

    protected

    # Connect the underlying socket
    #
    # @return [nil]
    def connect!
      debug 'Initializing connection'
      unless(@connecting)
        @connecting = true
        if(socket && socket.alive?)
          socket.terminate
          @socket = nil
        end
        @socket = Ksocket.new(:host => host, :port => port)
        self.link socket
        socket.put version.rjust(4).upcase
        identify_and_negotiate
        info 'Connection initialized'
        @connecting = false
      end
      nil
    end

  end
end
