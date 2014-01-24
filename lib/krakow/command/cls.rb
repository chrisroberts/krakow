module Krakow
  class Command
    class Cls < Command

      def initialize(args={})
        super
      end

      def to_line
        "#{name}\n"
      end

    end
  end
end
