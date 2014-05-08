require 'krakow'

module Krakow
  # Received message
  # @abstract
  class FrameType

    autoload :Error, 'krakow/frame_type/error'
    autoload :Message, 'krakow/frame_type/message'
    autoload :Response, 'krakow/frame_type/response'

    include Utils::Lazy
    # @!parse include Krakow::Utils::Lazy::InstanceMethods
    # @!parse extend Krakow::Utils::Lazy::ClassMethods

    # Registered frame types
    FRAME_TYPE_MAP = [
      FrameType::Response,
      FrameType::Error,
      FrameType::Message
    ]
    # Size bytes
    SIZE_BYTES = 4

    class << self

      # Information about incoming frame
      # @param bytes [String]
      # @return [Hash]
      def decode(bytes)
        size, type = bytes.unpack('l>l>')
        {:size => size - SIZE_BYTES, :type => type}
      end

      # Build proper FrameType instance based on args
      # @param args [Hash]
      # @option args [FrameType] :type class of frame
      # @option args [String] :data
      # @option args [Integer] :size
      # @return [FrameType]
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

    # Content of message
    #
    # @return [String]
    def content
      raise NotImplementedError.new 'Content method not properly defined!'
    end

  end
end
