module Krakow
  class Command
    class Cls < Command

      def initialize(args={})
        super
      end

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
