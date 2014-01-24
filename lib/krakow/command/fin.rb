module Krakow
  class Command
    class Fin < Command

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
