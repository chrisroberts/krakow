require 'krakow'
require 'celluloid/io'

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

    include Celluloid::IO

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

    finalizer :goodbye_my_love!

    # @return [Hash] current configuration for endpoint
    attr_reader :endpoint_settings
    # @return [Socket-ish] underlying socket like instance
    attr_reader :socket

    attr_reader :connector, :reconnector, :reconnect_notifier, :responder, :running

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
    attribute :queue, Queue, :default => ->{ Queue.new }
    attribute :callbacks, Hash, :default => ->{ Hash.new }
    attribute :responses, Queue, :default => ->{ Queue.new }
    attribute :notifier, Celluloid::Actor
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
      @connector = Mutex.new
      @reconnector = Mutex.new
      @responder = Mutex.new
      @reconnect_notifier = Celluloid::Signals.new
      @socket_retries = 0
      @socket_max_retries = 10
      @reconnect_pause = 0.5
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
      connector.synchronize do
        connect!
      end
      nil
    end

    # Send message to remote server
    #
    # @param message [Krakow::Message] message to send
    # @return [TrueClass, Krakow::FrameType] response if expected or true
    def transmit(message)
      output = message.to_line
      response_wait = wait_time_for(message)
      if(response_wait > 0)
        transmit_with_response(message, response_wait)
      else
        debug ">>> #{output}"
        safe_socket{|socket| socket.write output }
        true
      end
    end

    # Sends message and waits for response
    #
    # @param message [Krakow::Message] message to send
    # @return [Krakow::FrameType] response
    def transmit_with_response(message, wait_time)
      responder.synchronize do
        safe_socket{|socket| socket.write(message.to_line) }
        responses.clear
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
    end

    # Destructor method for cleanup
    #
    # @return [nil]
    def goodbye_my_love!
      debug 'Tearing down connection'
      if(socket && !socket.closed?)
        [lambda{ socket.write Command::Cls.new.to_line}, lambda{socket.close}].each do |action|
          begin
            action.call
          rescue IOError, SystemCallError => e
            warn "Socket error encountered during teardown: #{e.class}: #{e}"
          end
        end
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
      buf = socket.recv(8)
      if(buf)
        @receiving = true
        debug "<<< #{buf.inspect}"
        struct = FrameType.decode(buf)
        debug "Decoded structure: #{struct.inspect}"
        struct[:data] = socket.read(struct[:size])
        debug "<<< #{struct[:data].inspect}"
        @receiving = false
        frame = FrameType.build(struct)
        debug "Struct: #{struct.inspect} Frame: #{frame.inspect}"
        frame
      else
        if(socket.closed?)
          abort Error::ConnectionUnavailable.new("#{self} encountered closed socket!")
        end
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
      @running = true
      while(@running)
        begin
          message = handle(receive)
          if(message)
            debug "Adding message to queue #{message}"
            queue << message
            notifier.signal(message) if notifier
          end
        rescue Error::ConnectionUnavailable => e
          warn "Failed to receive message: #{e.class} - #{e}"
          @running = false
          async.reconnect!
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
        callback[:actor].send(callback[:method], *(args + [current_actor]))
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
      safe_socket{|socket| socket.write(ident.to_line) }
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
      socket && !socket.closed?
    end

    protected

    # Destruct the underlying socket
    #
    # @return [nil]
    def teardown_socket
      if(socket && (socket.closed? || socket.eof?))
        socket.close unless socket.closed?
        @socket = nil
        warn 'Existing socket instance has been destroyed from this connection'
      end
      nil
    end

    # Provides socket failure state handling around given block
    #
    # @yield [socket] execute within socket safety layer
    # @yieldparam [socket] underlying socket
    # @return [Object] result of executed block
    def safe_socket(*args)
      begin
        if(socket.nil? || socket.closed?)
          raise Error::ConnectionUnavailable.new 'Current connection is closed!'
        end
        result = yield socket if block_given?
        result
      rescue Error::ConnectionUnavailable, SystemCallError, IOError => e
        warn "Safe socket encountered error (socket in failed state): #{e.class}: #{e}"
        reconnect!
        retry
      rescue Celluloid::Error => e
        warn "Internal error encountered. Allowing exception to bubble. #{e.class}: #{e}"
        abort e
      rescue Exception => e
        warn "!!! Unexpected error encountered within safe socket: #{e.class}: #{e}"
        raise
      end
    end

    # Reconnect the underlying socket
    #
    # @return [nil]
    def reconnect!
      if(reconnector.try_lock)
        begin
          if(@socket_max_retries <= @socket_retries)
            abort ConnectionFailure.new "Failed to re-establish connection after #{@socket_retries} tries."
          end
          pause_interval = @reconnect_pause * @socket_retries
          @socket_retries += 1
          warn "Pausing for #{pause_interval} seconds before reconnect"
          sleep(pause_interval)
          init!
          @socket_retries = 0
        rescue Celluloid::Error => e
          warn "Internal error encountered. Allowing exception to bubble. #{e.class}: #{e}"
          abort e
        rescue SystemCallError, IOError => e
          error "Reconnect error encountered: #{e.class} - #{e}"
          retry
        end
        callback_for(:reconnect)
        reconnect_notifier.broadcast(:connected)
        reconnector.unlock
      else
        reconnect_notifier.wait(:connected)
      end
      nil
    end

    # Connect the underlying socket
    #
    # @return [nil]
    def connect!
      debug 'Initializing connection'
      if(@socket)
        @socket.close unless @socket.closed?
        @socket = nil
      end
      @socket = Celluloid::IO::TCPSocket.new(host, port)
      safe_socket{|socket| socket.write version.rjust(4).upcase}
      identify_and_negotiate
      async.process_to_queue!
      info 'Connection initialized'
      nil
    end

  end
end
