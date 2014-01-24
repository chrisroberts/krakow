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

    end
  end
end
