require 'krakow'

module Krakow
  module Utils
    # Adds functionality to facilitate laziness
    module Lazy

      include Utils::Logging

      # Instance methods for laziness
      module InstanceMethods

        # @return [Hash] argument hash
        attr_reader :arguments

        # Create new instance
        #
        # @param args [Hash]
        # @return [Object]
        def initialize(args={})
          @arguments = {}.tap do |hash|
            self.class.attributes.each do |name, options|
              val = args[name]
              if(options[:required] && !args.has_key?(name))
                raise ArgumentError.new("Missing required option: `#{name}`")
              end
              if(val && options[:type] && !(valid = [options[:type]].flatten.compact).detect{|k| val.is_a?(k)})
                raise TypeError.new("Invalid type for option `#{name}` (#{val} <#{val.class}>). Valid - #{valid.map(&:to_s).join(',')}")
              end
              if(val.nil? && options[:default] && !args.has_key?(name))
                val = options[:default].respond_to?(:call) ? options[:default].call : options[:default]
              end
              hash[name] = val
            end
          end
        end
        alias_method :super_init, :initialize

        # @return [String]
        def to_s
          "<#{self.class.name}:#{object_id}>"
        end

        # @return [String]
        def inspect
          "<#{self.class.name}:#{object_id} [#{arguments.inspect}]>"
        end

      end

      # Class methods for laziness
      module ClassMethods

        # Add new attributes to class
        #
        # @param name [String]
        # @param type [Class, Array<Class>]
        # @param options [Hash]
        # @option options [true, false] :required must be provided on initialization
        # @option options [Object, Proc] :default default value
        # @return [nil]
        def attribute(name, type, options={})
          name = name.to_sym
          attributes[name] = {:type => type}.merge(options)
          define_method(name) do
            arguments[name.to_sym]
          end
          define_method("#{name}?") do
            !!arguments[name.to_sym]
          end
          nil
        end

        # Return attributes
        #
        # @param args [Symbol] :required or :optional
        # @return [Array<Hash>]
        def attributes(*args)
          @attributes ||= {}
          if(args.include?(:required))
            Hash[@attributes.find_all{|k,v| v[:required]}]
          elsif(args.include?(:optional))
            Hash[@attributes.find_all{|k,v| !v[:required]}]
          else
            @attributes
          end
        end

        # Directly set attribute hash
        #
        # @param attrs [Hash]
        # @return [TrueClass]
        # @todo need deep dup here
        def set_attributes(attrs)
          @attributes = attrs.dup
          true
        end

      end

      class << self

        # Injects laziness into class
        #
        # @param klass [Class]
        def included(klass)
          klass.class_eval do
            include InstanceMethods
            extend ClassMethods

            class << self

              def inherited(klass)
                klass.set_attributes(self.attributes)
              end

            end
          end
        end

      end

    end
  end
end
