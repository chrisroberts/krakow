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

      class << self
        def error
          %w(E_INVALID)
        end
      end

    end
  end
end
