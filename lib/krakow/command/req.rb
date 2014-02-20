module Krakow
  class Command
    class Req < Command

      def initialize(args={})
        super
        required! :message_id, :timeout
      end

      def to_line
        "#{name} #{message_id} #{self.timeout}\n"
      end

      class << self
        def error
          %w(E_INVALID E_REQ_FAILED)
        end
      end

    end
  end
end
