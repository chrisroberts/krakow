require 'krakow'

module Krakow
  class FrameType
    # Error from server
    class Error < FrameType

      # @!group Attributes

      # @!macro [attach] attribute
      #   @!method $1
      #     @return [$2] the $1 $0
      #   @!method $1?
      #     @return [TrueClass, FalseClass] truthiness of the $1 $0
      attribute :error, String, :required => true

      # @!endgroup

      # @return [String] content of error
      def content
        error
      end

    end
  end
end
