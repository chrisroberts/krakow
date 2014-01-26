
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
