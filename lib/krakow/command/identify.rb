require 'multi_json'
require 'krakow'

module Krakow
  class Command
    # Update client metadata on server / negotiate features
    class Identify < Command

      # @!group Attributes

      # @!macro [attach] attribute
      #   @!method $1
      #     @return [$2] the $1 $0
      #   @!method $1?
      #     @return [TrueClass, FalseClass] truthiness of the $1 $0
      attribute :short_id, [String, Numeric], :required => true
      attribute :long_id, [String, Numeric], :required => true
      attribute :feature_negotiation, [TrueClass, FalseClass]
      attribute :heartbeat_interval, Numeric
      attribute :output_buffer_size, Integer
      attribute :output_buffer_timeout, Integer
      attribute :tls_v1, [TrueClass, FalseClass]
      attribute :snappy, [TrueClass, FalseClass]
      attribute :deflate, [TrueClass, FalseClass]
      attribute :deflate_level, Integer
      attribute :sample_rate, Integer

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
        [name, "\n", payload.bytesize, payload].pack('a*a*l>a*')
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
