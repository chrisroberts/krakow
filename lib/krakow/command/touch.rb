module Krakow
  class Command
    class Touch < Command

      def initialize(args={})
        super
        required! :message_id
      end

      def to_line
        "#{name} #{message_id}\n"
      end

    end
  end
end
