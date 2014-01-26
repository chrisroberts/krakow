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

      class << self
        def error
          %w(E_INVALID E_FIN_FAILED)
        end
      end

    end
  end
end
