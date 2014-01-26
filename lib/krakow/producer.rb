module Krakow
  class Producer

    include Utils::Lazy
    include Celluloid

    trap_exit  :connection_failure
    finalizer :goodbye_my_love!

    attr_reader :connection

    def initialize(args={})
      super
      required! :host, :port, :topic
      optional :connect_retries
      arguments[:reconnect_retries] ||= 10
      arguments[:reconnect_interval] = 5
      connect
    end

    # Establish connection to configured `host` and `port`
    def connect
      info "Establishing connection to: #{host}:#{port}"
      begin
        @connection = Connection.new(:host => host, :port => port)
        self.link connection
        connection.init!
        info "Connection established: #{connection}"
      rescue => e
        abort e
      end
    end

    def to_s
      "<#{self.class.name}:#{object_id} {#{host}:#{port}} T:#{topic}>"
    end

    # Return if connected
    def connected?
      connection && connection.alive?
    end

    # Process connection failure and attempt reconnection
    def connection_failure(*args)
      warn "Connection has failed to #{host}:#{port}"
      retries = 0
      begin
        connect
      rescue => e
        retries += 1
        warn "Connection retry #{retries}/#{reconnect_retries} failed. #{e.class}: #{e}"
        if(retries < reconnect_retries)
          sleep_interval = retries * reconnect_interval
          debug "Sleeping for reconnect interval of #{sleep_interval} seconds"
          sleep sleep_interval
          retry
        else
          abort e
        end
      end
    end

    def goodbye_my_love!
      debug 'Tearing down producer'
      if(connection && connection.alive?)
        connection.terminate
      end
      @connection = nil
      info 'Producer torn down'
    end

    # message:: Message to send
    # Write message
    def write(*message)
      if(connection.alive?)
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
        read(:validate)
      else
        abort Error.new 'Remote connection is unavailable!'
      end
    end

    # args:: Options (:validate)
    # Read response from connection. If :validate is included an
    # exception will be raised if `FrameType::Error` is received
    def read(*args)
      result = connection.responses.pop
      debug "Read response: #{result}"
      if(args.include?(:validate) && result.is_a?(FrameType::Error))
        error = Error::BadResponse.new('Write failed')
        error.result = result
        abort error
      else
        result
      end
    end

  end
end
