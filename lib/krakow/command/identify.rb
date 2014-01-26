require 'multi_json'

module Krakow
  class Command
    class Identify < Command

      def initialize(args={})
        super
        required! :short_id, :long_id
        optional(
          :feature_negotiation, :heartbeat_interval, :output_buffer_size,
          :output_buffer_timeout, :tls_v1, :snappy, :deflate, :deflate_level,
          :sample_rate
        )
      end

      def to_line
        payload = MultiJson.dump(arguments)
        [name, "\n", payload.length, payload].pack('a*a*l>a*')
      end

      class << self
        def ok
          %w(OK)
        end

        def error
          %w(E_INVALID E_BAD_BODY)
        end
      end

    end
  end
end
