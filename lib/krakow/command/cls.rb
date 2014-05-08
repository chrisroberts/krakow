require 'krakow'

module Krakow
  class Command
    # Close connection
    class Cls < Command

      def to_line
        "#{name}\n"
      end

      class << self
        def ok
          %w(CLOSE_WAIT)
        end

        def error
          %w(E_INVALID)
        end
      end

    end
  end
end
