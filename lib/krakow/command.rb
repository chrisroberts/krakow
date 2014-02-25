module Krakow
  class Command

    include Utils::Lazy

    class << self

      def ok
        []
      end

      def error
        []
      end

      # message:: Krakow::Message
      # Returns type of response expected (:none, :error_only, :required)
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

    attr_accessor :response

    # Return command name
    def name
      self.class.name.split('::').last.upcase
    end

    # Convert to line output
    def to_line(*args)
      raise NotImplementedError.new 'No line conversion method defined!'
    end

    def ok?(response)
      response = response.content if response.is_a?(FrameType)
      self.class.ok.include?(response)
    end

    def error?(response)
      response = response.content if response.is_a?(FrameType)
      self.class.error.include?(response)
    end

    # Make all the commands available
    Dir.glob(File.join(File.dirname(__FILE__), 'command', '*')).each do |path|
      autoload(
        File.basename(path).sub(File.extname(path), '').capitalize.to_sym,
        File.join('krakow/command', File.basename(path).sub(File.extname(path), ''))
      )
    end

  end
end
