module Krakow
  class Command
    class Sub < Command

      def initialize(args={})
        super
        required! :topic_name, :channel_name
      end

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
