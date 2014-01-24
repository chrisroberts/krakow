module Krakow
  class Command
    class Rdy < Command

      def initialize(args={})
        super
        required! :count
      end

      def to_line
        "#{name} #{count}\n"
      end

    end
  end
end
