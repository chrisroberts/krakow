require 'krakow'

module Krakow
  # Base error type
  class Error < StandardError

    # Failed to enable required feature on connection
    class ConnectionFeatureFailure < Error; end
    # Failed to perform lookup (not found)
    class LookupFailed < Error; end
    # Connection has failed
    class ConnectionFailure < Error; end
    # Configuration is not in valid state
    class ConfigurationError < Error; end
    # Connection is temporarily unavailable
    class ConnectionUnavailable < Error; end
    # Consumer was not set
    class OriginNotFound < Error; end

    # Invalid response
    class BadResponse < Error
      # @return [Response] error response
      attr_accessor :result
      # No response received
      class NoResponse < BadResponse
      end
    end

  end
end
