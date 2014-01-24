module Krakow
  class FrameType
    class Response < FrameType

      def initialize(args={})
        super
        required! :response
      end

    end
  end
end
