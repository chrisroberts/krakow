module Krakow
  class Producer

    autoload :Http, 'krakow/producer/http'

    include Utils::Lazy
    include Celluloid

    trap_exit  :connection_failure
    finalizer :goodbye_my_love!

    attr_reader :connection

    def initialize(args={})
      super
      required! :host, :port, :topic
      optional :reconnect_retries, :reconnect_interval, :connection_options
      arguments[:reconnect_retries] ||= 10
      arguments[:reconnect_interval] ||= 5
      arguments[:connection_options] = {:features => {}, :config => {}}.merge(
        arguments[:connection_options] || {}
      )
      connect
    end

    # Establish connection to configured `host` and `port`
    def connect
      info "Establishing connection to: #{host}:#{port}"
      begin
        @connection = Connection.new(
          :host => host,
          :port => port,
          :features => connection_options[:features],
          :features_args => connection_options[:config]
        )
        connection.init!
        self.link connection
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
      !!(connection && connection.alive?)
    end

    # Process connection failure and attempt reconnection
    def connection_failure(*args)
      @connection = nil
      begin
        warn "Connection failure detected for #{host}:#{port}"
        connect
      rescue => e
        warn "Failed to establish connection to #{host}:#{port}. Pausing #{reconnect_interval} before retry"
        sleep reconnect_interval
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
      abort ArgumentError.new 'Expecting one or more messages to send. None provided.'
      if(connection && connection.alive?)
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
      else
        abort Error::ConnectionUnavailable.new 'Remote connection is unavailable!'
      end
    end

  end
end
