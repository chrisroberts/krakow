require 'krakow'

module Krakow
  class Command
    # Update RDY state
    class Rdy < Command

      # @!group Attributes

      # @!macro [attach] attribute
      #   @!method $1
      #     @return [$2] the $1 $0
      #   @!method $1?
      #     @return [TrueClass, FalseClass] truthiness of the $1 $0
      attribute :count, Integer, :required => true

      # @!endgroup

      def to_line
        "#{name} #{count}\n"
      end

      class << self
        def error
          %w(E_INVALID)
        end
      end

    end
  end
end
