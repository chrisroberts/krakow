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
          key = key.to_sym
          unless(arguments.has_key?(key))
            raise ArgumentError.new "Missing required option `#{key}`!"
          end
          define_singleton_method(key) do
            arguments[key]
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
          define_singleton_method(key) do
            arguments[key]
          end
        end
      end

      def to_s
        "<#{self.class.name}:#{object_id}>"
      end

      def inspect
        "<#{self.class.name}:#{object_id} [#{arguments.inspect}]>"
      end

    end
  end
end
