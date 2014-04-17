module Krakow
  class Command
    class Mpub < Command

      def initialize(args={})
        super
        required! :topic_name, :messages
        arguments[:messages] = [messages].flatten.compact.map(&:to_s)
      end

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
