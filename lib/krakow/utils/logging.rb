require 'krakow'
require 'logger'

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
          $krakow_logger ||= Logger.new(STDOUT)
        else
          severity, string = args
          ($krakow_logger ||= Logger.new(STDOUT)).send(severity.to_sym, "#{self}: #{string}")
          nil
        end
      end

      class << self
        # Set the logging output level
        #
        # @param level [Integer]
        # @return [Integer, nil]
        def level=(level)
          lgr = $krakow_logger ||= Logger.new(STDOUT)
          lgr.level = lgr.class.const_get(level.to_s.upcase.to_sym)
        end
      end

    end
  end
end
