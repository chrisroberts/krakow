module Krakow
  class Error < StandardError

    class LookupFailed < Error; end
    class ConnectionFailure < Error; end
    class ConfigurationError < Error; end

    class BadResponse < Error
      attr_accessor :result
    end

  end
end
