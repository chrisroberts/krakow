module Krakow
  class FrameType
    class Message < FrameType

      def initialize(args={})
        super
        required! :attempts, :timestamp, :message_id, :message
      end

      def content
        message
      end

    end
  end
end
