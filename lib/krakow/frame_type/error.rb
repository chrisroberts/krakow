module Krakow
  class FrameType
    class Error < FrameType

      def initialize(args={})
        super
        required! :error
      end

    end
  end
end
