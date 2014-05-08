require 'krakow'

module Krakow
  class FrameType
    # Message received from server
    class Message < FrameType

      # @return [Krakow::Consumer]
      attr_accessor :origin

      # @!group Properties

      # @!macro [attach] property
      #   @!method $1
      #     @return [$2] the $1 $0
      #   @!method $1?
      #     @return [TrueClass, FalseClass] truthiness of the $1 $0
      property :attempts, Integer, :required => true
      property :timestamp, Integer, :required => true
      property :message_id, String, :required => true
      property :message, String, :required => true

      # @!endgroup

      # Message content
      #
      # @return [String]
      def content
        message
      end

      # @return [Krakow::Consumer]
      def origin
        unless(@origin)
          error 'No origin has been specified for this message'
          abort Krakow::Error::OriginNotFound.new('No origin specified for this message')
        end
        @origin
      end

      # Proxy to [Krakow::Consumer#confirm]
      def confirm(*args)
        origin.confirm(*[self, *args].compact)
      end
      alias_method :finish, :confirm

      # Proxy to [Krakow::Consumer#requeue]
      def requeue(*args)
        origin.requeue(*[self, *args].compact)
      end

      # Proxy to [Krakow::Consumer#touch]
      def touch(*args)
        origin.touch(*[self, *args].compact)
      end

    end
  end
end
