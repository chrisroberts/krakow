require 'krakow'

module Krakow
  class Command
    # Subscribe to topic/channel
    class Sub < Command

      # @!group Properties

      # @!macro [attach] property
      #   @!method $1
      #     @return [$2] the $1 $0
      #   @!method $1?
      #     @return [TrueClass, FalseClass] truthiness of the $1 $0
      property :topic_name, String, :required => true
      property :channel_name, String, :required => true

      # @!endgroup

      def to_line
        "#{name} #{topic_name} #{channel_name}\n"
      end

      class << self
        def ok
          %w(OK)
        end

        def error
          %w(E_INVALID E_BAD_TOPIC E_BAD_CHANNEL)
        end
      end

    end
  end
end
