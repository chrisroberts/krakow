module Krakow
  class FrameType
    class Message < FrameType

      attr_accessor :origin

      def initialize(args={})
        super
        required! :attempts, :timestamp, :message_id, :message
      end

      def content
        message
      end

      def origin
        unless(@origin)
          error 'No origin has been specified for this message'
          abort Krakow::Error::OriginNotFound.new('No origin specified for this message')
        end
        @origin
      end

      def confirm(*args)
        origin.confirm(*[self, *args].compact)
      end
      alias_method :finish, :confirm

      def requeue(*args)
        origin.requeue(*[self, *args].compact)
      end

      def touch(*args)
        origin.touch(*[self, *args].compact)
      end

    end
  end
end
