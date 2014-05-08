require 'krakow'

module Krakow
  class FrameType
    # Response from server
    class Response < FrameType

      # @!group Properties

      # @!macro [attach] property
      #   @!method $1
      #     @return [$2] the $1 $0
      #   @!method $1?
      #     @return [TrueClass, FalseClass] truthiness of the $1 $0
      property :response, String, :required => true

      # @!endgroup

      # @return [String] content of response
      def content
        response
      end

    end
  end
end
