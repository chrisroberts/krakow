require 'krakow'

module Krakow
  class Command
    # Publish multiple messages
    class Mpub < Command

      # @!group Attributes

      # @!macro [attach] attribute
      #   @!method $1
      #     @return [$2] the $1 $0
      #   @!method $1?
      #     @return [TrueClass, FalseClass] truthiness of the $1 $0
      attribute :topic_name, String, :required => true
      attribute :messages, [Array, String], :required => true

      # @!endgroup
      def to_line
        formatted_messages = messages.map do |message|
          [message.length, message].pack('l>a*')
        end.join
        [name, ' ', topic_name, "\n", formatted_messages.length, messages.size, formatted_messages].pack('a*a*a*a*l>l>a*')
      end

      class << self
        def ok
          %w(OK)
        end

        def error
          %w(E_INVALID E_BAD_TOPIC E_BAD_BODY E_BAD_MESSAGE E_MPUB_FAILED)
        end
      end

    end
  end
end
