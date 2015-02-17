require 'krakow'

module Krakow
  class Command
    # Publish single message
    class Auth < Command

      # @!group Attributes

      # @!macro [attach] attribute
      #   @!method $1
      #     @return [$2] the $1 $0
      #   @!method $1?
      #     @return [TrueClass, FalseClass] truthiness of the $1 $0
      attribute :secret, String, :required => true

      # @!endgroup

      def to_line
        scrt = secret.to_s
        [name, "\n", scrt.bytesize, scrt].pack('a*a*a*a*l>a*')
      end

      class << self
        def ok
          %w(OK)
        end

        def error
          %w(E_AUTH_FAILED E_UNAUTHORIZED)
        end
      end

    end
  end
end
