
module Krakow
  class Command
    class Pub < Command

      def initialize(args={})
        super
        required! :topic_name, :message
      end

      def to_line
        [name, ' ', topic_name, "\n", message.length, message].pack('a*a*a*a*l>a*')
      end

    end
  end
end
