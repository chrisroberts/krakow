module Krakow
  class FrameType

    autoload :Error, 'krakow/frame_type/error'
    autoload :Message, 'krakow/frame_type/message'
    autoload :Response, 'krakow/frame_type/response'

    include Utils::Lazy

    FRAME_TYPE_MAP = [
      FrameType::Response,
      FrameType::Error,
      FrameType::Message
    ]
    SIZE_BYTES = 4

    class << self

      # bytes:: 8 bytes
      # Return information about incoming frame
      def decode(bytes)
        size, type = bytes.unpack('l>l>')
        {:size => size - SIZE_BYTES, :type => type}
      end

      # args:: arguments (:type, :data, :size)
      # Build proper FrameType instance based on args
      def build(args={})
        klass = FRAME_TYPE_MAP[args[:type].to_i]
        if(klass == FrameType::Response)
          klass.new(:response => args[:data])
        elsif(klass == FrameType::Error)
          klass.new(:error => args[:data])
        elsif(klass == FrameType::Message)
          unpacked = args[:data].unpack("Q>s>a16a#{args[:size]}")
          klass.new(
            Hash[*([:timestamp, :attempts, :message_id, :message].zip(unpacked).flatten)]
          )
        else
          raise TypeError.new "Unknown frame type received: #{args[:type].inspect} - #{klass.inspect}"
        end
      end
    end

    def initialize(args={})
      super
    end

    def content
      raise NoMethodError.new 'Content method not properly defined!'
    end

  end
end
