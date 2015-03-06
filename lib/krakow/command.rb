require 'krakow'

module Krakow
  # Messages for sending to remote server
  class Command

    include Utils::Lazy
    # @!parse include Utils::Lazy::InstanceMethods
    # @!parse extend Utils::Lazy::ClassMethods

    autoload :Auth, 'krakow/command/auth'
    autoload :Cls, 'krakow/command/cls'
    autoload :Fin, 'krakow/command/fin'
    autoload :Identity, 'krakow/command/identity'
    autoload :Mpub, 'krakow/command/mpub'
    autoload :Nop, 'krakow/command/nop'
    autoload :Pub, 'krakow/command/pub'
    autoload :Rdy, 'krakow/command/rdy'
    autoload :Req, 'krakow/command/req'
    autoload :Sub, 'krakow/command/sub'
    autoload :Touch, 'krakow/command/touch'

    class << self

      # Allowed OK return values
      #
      # @return [Array<String>]
      def ok
        []
      end

      # Allowed ERROR return values
      #
      # @return [Array<String>]
      def error
        []
      end

      # Response type expected
      #
      # @param message [Krakow::Message] message to check
      # @return [Symbol] response expected (:none, :error_only, :required)
      def response_for(message)
        if(message.class.ok.empty?)
          if(message.class.error.empty?)
            :none
          else
            :error_only
          end
        else
          :required
        end
      end

    end

    # @return [Krakow::FrameType] response to command
    attr_accessor :response

    # @return [String] name of command
    def name
      self.class.name.split('::').last.upcase
    end

    # Convert to line output
    #
    # @return [String] socket ready string
    def to_line(*args)
      raise NotImplementedError.new 'No line conversion method defined!'
    end

    # Is response OK
    #
    # @return [TrueClass, FalseClass]
    def ok?(response)
      response = response.content if response.is_a?(FrameType)
      self.class.ok.include?(response)
    end

    # Is response ERROR
    #
    # @return [TrueClass, FalseClass]
    def error?(response)
      response = response.content if response.is_a?(FrameType)
      self.class.error.include?(response)
    end

  end
end
