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

    attr_reader :socket, :endpoint_settings

    def initialize(args={})
      super
      required! :host, :port
      optional :version, :queue, :callback, :responses, :notifier, :features, :response_wait, :error_wait, :disable_negotiation
      arguments[:queue] ||= Queue.new
      arguments[:responses] ||= Queue.new
      arguments[:version] ||= 'v2'
      arguments[:features] ||= {}
      arguments[:response_wait] ||= 2
      arguments[:error_wait] ||= 2
      @socket = TCPSocket.new(host, port)
      @endpoint_settings = {}
    end

    def to_s
      "<#{self.class.name}:#{object_id} {#{host}:#{port}}>"
    end

    # Initialize the connection
    def init!
      debug 'Initializing connection'
      socket.write version.rjust(4).upcase
      unless(disable_negotiation?)
        identify_and_negotiate
      end
      async.process_to_queue!
      info 'Connection initialized'
    end

    # message:: Command instance to send
    # Send the message
    # TODO: Do we want to validate Command instance and abort if
    # response is already set?
    def transmit(message)
      output = message.to_line
      debug ">>> #{output}"
      socket.write output
      response_wait = wait_time_for(message)
      responses.clear if response_wait
      if(response_wait)
        response = nil
        (response_wait / 0.1).to_i.times do |i|
          response = responses.pop unless responses.empty?
          break if response
          debug "Response wait sleep for 0.1 seconds (#{i+1} time)"
          sleep(0.1)
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
          abort Error::BadResponse::NoResponse.new "No response provided for message #{message}"
        end
      else
        true
      end
    end

    # Cleanup prior to destruction
    def goodbye_my_love!
      debug 'Tearing down connection'
      if(socket && !socket.closed?)
        socket.write Command::Cls.new.to_line
        socket.close
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
          raise Error.new("#{self} encountered closed socket!")
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
      loop do
        message = handle(receive)
        if(message)
          debug "Adding message to queue #{message}"
          queue << message
          notifier.signal(message) if notifier
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
        if(callback && callback[:actor] && callback[:method])
          debug "Sending #{message} to callback `#{callback[:actor]}##{callback[:method]}`"
          message = callback[:actor].send(callback[:method], message, current_actor)
        end
        if(!message.is_a?(FrameType::Message))
          debug "Captured non-message type response: #{message}"
          responses << message
          nil
        else
          message
        end
      end
    end

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
          :short_id => 'fubarx',
          :long_id => 'fubarx',
          :user_agent => "krakow/#{Krakow::VERSION}",
          :feature_negotiation => !disable_negotiation
        }
      end
      @identify_defaults
    end

    def identify_and_negotiate
      ident = Command::Identify.new(
        identify_defaults.merge(features)
      )
      socket.write(ident.to_line)
      response = receive
      begin
        @endpoint_settings = MultiJson.load(response.content, :symbolize_keys => true)
        info "Connection settings: #{endpoint_settings.inspect}"
        # Enable things we need to enable
        ENABLEABLE_FEATURES.each do |key|
          if(endpoint_settings[key])
            send(key)
          end
        end
      rescue MultiJson::LoadError => e
        error "Failed to parse response from Identify request: #{e} - #{response}"
        abort e
      end
      true
    end

    def snappy
      info 'Loading support for snappy compression and converting connection'
      require 'krakow/utils/snappy_frames'
      @socket = SnappyFrames::Io.new(socket)
      response = receive
      info "Snappy connection conversion complete. Response: #{response.inspect}"
    end

    def deflate
      debug 'Loading support for deflate compression and converting connection'
      raise NotImplementedError
    end

    def tls_v1
      info 'Enabling TLS for connection'
      @socket = Celluloid::IO::SSLSocket.new(@socket)
      @socket.define_singleton_method(:recv) do |len|
        str = readpartial(len)
        if(len > str.length)
          str << sysread(len - str.length)
        end
        str
      end
      @socket.sync = true
      @socket.connect
      response = receive
      info "TLS enable complete. Response: #{response.inspect}"
    end

  end
end
