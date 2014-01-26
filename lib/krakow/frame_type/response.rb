module Krakow
  class FrameType
    class Response < FrameType

      def initialize(args={})
        super
        required! :response
      end

      def content
        response
      end

    end
  end
end
