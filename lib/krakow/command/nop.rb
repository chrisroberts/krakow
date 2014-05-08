require 'krakow'

module Krakow
  class Command
    # No-op
    class Nop < Command

      def to_line
        "#{name}\n"
      end

    end
  end
end
