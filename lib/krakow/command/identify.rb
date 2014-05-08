require 'multi_json'
require 'krakow'

module Krakow
  class Command
    # Update client metadata on server / negotiate features
    class Identify < Command

      # @!group Properties

      # @!macro [attach] property
      #   @!method $1
      #     @return [$2] the $1 $0
      #   @!method $1?
      #     @return [TrueClass, FalseClass] truthiness of the $1 $0
      property :short_id, [String, Numeric], :required => true
      property :long_id, [String, Numeric], :required => true
      property :feature_negotiation, [TrueClass, FalseClass]
      property :heartbeat_interval, Numeric
      property :output_buffer_size, Integer
      property :output_buffer_timeout, Integer
      property :tls_v1, [TrueClass, FalseClass]
      property :snappy, [TrueClass, FalseClass]
      property :deflate, [TrueClass, FalseClass]
      property :deflate_level, Integer
      property :sample_rate, Integer

      # @!endgroup

      def to_line
        filtered = Hash[*
          arguments.map do |key, value|
            unless(value.nil?)
              [key, value]
            end
          end.compact.flatten
        ]
        payload = MultiJson.dump(filtered)
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
