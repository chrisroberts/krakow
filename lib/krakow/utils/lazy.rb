module Krakow
  module Utils
    module Lazy

      include Utils::Logging

      attr_reader :arguments

      def initialize(args={})
        @arguments = {}.tap do |hash|
          args.each do |k,v|
            hash[k.to_sym] = v
          end
        end
      end

      # args:: list of required keys
      # Check that required keys exist in `arguments` hash. Raise
      # error if not found
      def required!(*args)
        args.each do |key|
          unless(arguments.has_key?(key.to_sym))
            raise ArgumentError.new "Missing required option `#{key}`!"
          end
        end
      end

      # args:: list of required keys
      # Optional keys for arguments
      def optional(*args)
        args.each do |key|
          key = key.to_sym
          unless(arguments.has_key?(key))
            arguments[key] = nil
          end
        end
      end

      def to_s
        "<#{self.class.name}:#{object_id}>"
      end

      def inspect
        "<#{self.class.name}:#{object_id} [#{arguments.inspect}]>"
      end

      def method_missing(*args)
        key = args.first.to_sym
        if(arguments.has_key?(key))
          arguments[key]
        else
          super
        end
      end

      def respond_to_missing?(key, *args)
        key = key.to_sym
        super || arguments.has_key?(key)
      end

    end
  end
end
