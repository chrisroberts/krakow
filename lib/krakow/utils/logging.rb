require 'krakow'

module Krakow
  module Utils
    # Logging helpers
    module Logging

      # Define base logging types
      %w(debug info warn error).each do |key|
        define_method(key) do |string|
          log(key, string)
        end
      end

      # Log message
      #
      # @param args [Array, nil]
      # @return [Logger, nil]
      def log(*args)
        if(args.empty?)
          Celluloid::Logger
        else
          severity, string = args
          Celluloid::Logger.send(severity.to_sym, "#{self}: #{string}")
          nil
        end
      end

      class << self
        # Set the logging output level
        #
        # @param level [Integer]
        # @return [Integer, nil]
        def level=(level)
          if(Celluloid.logger.class == Logger)
            Celluloid.logger.level = Logger.const_get(level.to_s.upcase.to_sym)
          end
        end
      end

    end
  end
end
