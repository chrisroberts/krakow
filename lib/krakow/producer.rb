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
    end

    def goodbye_my_love!
      if(connection)
        connection.terminate
      end
      @connection = nil
    end

    # message:: Message to send
    # Write message
    def write(*message)
      if(message.size > 1)
        connection.transmit(
          Command::Mpub.new(
            :topic_name => topic,
            :messages => message
          )
        )
      else
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
      result = connection.queue.pop
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
