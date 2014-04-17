module Krakow
  class Error < StandardError

    class ConnectionFeatureFailure < Error; end
    class LookupFailed < Error; end
    class ConnectionFailure < Error; end
    class ConfigurationError < Error; end
    class ConnectionUnavailable < Error; end
    class OriginNotFound < Error; end

    class BadResponse < Error
      attr_accessor :result
      class NoResponse < BadResponse
      end
    end

  end
end
