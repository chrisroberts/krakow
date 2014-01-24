module Krakow
  class Command
    class Req < Command

      def initialize(args={})
        super
        required! :message_id, :timeout
      end

      def to_line
        "#{name} #{message_id} #{timeout}\n"
      end

    end
  end
end
