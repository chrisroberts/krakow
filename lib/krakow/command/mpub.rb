module Krakow
  class Command
    class Mpub < Command

      def initialize(args={})
        super
        required! :topic_name, :messages
      end

      def to_line
        formatted_messages = messages.map do |message|
          [message.length, message].pack('l>a*')
        end.join
        [name, ' ', topic_name, "\n", formatted_messages.length, messages.size, formatted_messages].pack('a*a*a*a*l>l>a*')
      end

    end
  end
end
