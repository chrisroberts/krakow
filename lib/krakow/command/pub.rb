require 'krakow'

module Krakow
  class Command
    # Publish single message
    class Pub < Command

      # @!group Attributes

      # @!macro [attach] attribute
      #   @!method $1
      #     @return [$2] the $1 $0
      #   @!method $1?
      #     @return [TrueClass, FalseClass] truthiness of the $1 $0
      attribute :topic_name, String, :required => true
      attribute :message, String, :required => true

      # @!endgroup

      def to_line
        [name, ' ', topic_name, "\n", message.length, message].pack('a*a*a*a*l>a*')
      end

      class << self
        def ok
          %w(OK)
        end

        def error
          %w(E_INVALID E_BAD_TOPIC E_BAD_MESSAGE E_PUB_FAILED)
        end
      end

    end
  end
end
