module Krakow
  module Utils
    module Logging

      # Define base logging types
      %w(debug info warn error).each do |key|
        define_method(key) do |string|
          log(key, string)
        end
      end

      # Log message
      def log(*args)
        if(args.empty?)
          Celluloid::Logger
        else
          severity, string = args
          Celluloid::Logger.send(severity.to_sym, "#{self}: #{string}")
        end
      end

      class << self
        def level=(level)
          if(Celluloid.logger.class == Logger)
            Celluloid.logger.level = Logger.const_get(level.to_s.upcase.to_sym)
          end
        end
      end

    end
  end
end
