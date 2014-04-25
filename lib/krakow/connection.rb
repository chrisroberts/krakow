require 'krakow/version'
require 'celluloid/io'
require 'celluloid/autostart'

module Krakow
  class Connection

    include Utils::Lazy
    include Celluloid::IO

    FEATURES = [
      :max_rdy_count,
      :max_msg_timeout,
      :msg_timeout,
      :tls_v1,
      :deflate,
      :deflate_level,
      :max_deflate_level,
      :snappy,
      :sample_rate
    ]
    EXCLUSIVE_FEATURES = [[:snappy, :deflate]]
    ENABLEABLE_FEATURES = [:tls_v1, :snappy, :deflate]

    finalizer :goodbye_my_love!

    attr_reader(
      :connector, :endpoint_settings, :reconnector,
      :reconnect_notifier, :responder, :running, :socket
    )

    def initialize(args={})
      super
      required! :host, :port
      optional(
        :version, :queue, :callbacks, :responses, :notifier,
        :features, :response_wait, :response_interval, :error_wait,
        :enforce_features, :features_args
      )
      arguments[:queue] ||= Queue.new
      arguments[:responses] ||= Queue.new
      arguments[:version] ||= 'v2'
      arguments[:features] ||= {}
      arguments[:features_args] ||= {}
      arguments[:response_wait] ||= 1
      arguments[:response_interval] ||= 0.01
      arguments[:error_wait] ||= 0.0
      arguments[:callbacks] ||= {}
      if(arguments[:enforce_features].nil?)
        arguments[:enforce_features] = true
      end
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

    def to_s
      "<#{self.class.name}:#{object_id} {#{host}:#{port}}>"
    end

    # Initialize the connection
    def init!
      connector.synchronize do
        connect!
      end
    end

    # message:: Command instance to send
    # Send the message
    # TODO: Do we want to validate Command instance and abort if
    # response is already set?
    # NOTE: Handle `Consumer` side via `Distribution` lookup
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

    # Cleanup prior to destruction
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
    end

    # Receive message and return proper FrameType instance
    def receive
      debug 'Read wait for frame start'
      buf = socket.recv(8)
      if(buf)
        @receiving = true
        debug "<<< #{buf.inspect}"
        struct = FrameType.decode(buf)
        debug "Decoded structure: #{struct.inspect}"
        struct[:data] = socket.recv(struct[:size])
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

    # Currently in the process of receiving a message
    def receiving?
      !!@receiving
    end

    # Pull message and queue
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
    end

    # message:: FrameType instance
    # Handle message if not an actual message
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
    # @param message [Command]
    # @return [Numeric] seconds to wait
    def wait_time_for(message)
      case Command.response_for(message)
      when :required
        response_wait
      when :error_only
        error_wait
      end
    end

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

    def snappy
      info 'Loading support for snappy compression and converting connection'
      @socket = ConnectionFeatures::SnappyFrames::Io.new(socket, features_args)
      response = receive
      info "Snappy connection conversion complete. Response: #{response.inspect}"
    end

    def deflate
      debug 'Loading support for deflate compression and converting connection'
      @socket = ConnectionFeatures::Deflate::Io.new(socket, features_args)
      response = receive
      info "Deflate connection conversion complete. Response: #{response.inspect}"
    end

    def tls_v1
      info 'Enabling TLS for connection'
      @socket = ConnectionFeatures::Ssl::Io.new(socket, features_args)
      response = receive
      info "TLS enable complete. Response: #{response.inspect}"
    end

    def connected?
      socket && !socket.closed?
    end

    protected

    def teardown_socket
      if(socket && (socket.closed? || socket.eof?))
        socket.close unless socket.closed?
        @socket = nil
        warn 'Existing socket instance has been destroyed from this connection'
      end
    end

    # Provides socket failure state handling around given block. Will
    # attempt reconnect and replay
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
    end

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
    end

  end
end
