module Krakow
  class FrameType
    class Error < FrameType

      def initialize(args={})
        super
        required! :error
      end

      def error
        arguments[:error]
      end

      def content
        error
      end

    end
  end
end
