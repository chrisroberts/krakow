module Krakow
  class Producer

    include Utils::Lazy
    include Celluloid

    finalizer :goodbye_my_love!

    attr_reader :connection

    def initialize(args={})
      super
      required! :host, :port, :topic
      @connection = Connection.new(:host => host, :port => port)
      connection.init!
      debug "Connection established: #{connection}"
    end

    def to_s
      "<#{self.class.name}:#{object_id} {#{host}:#{port}} T:#{topic}>"
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
