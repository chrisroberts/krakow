require 'krakow'

module Krakow
  # Consume messages from a server
  class Consumer

    class Queue

      include Zoidberg::Shell
      include Utils::Lazy

      # @return [Consumer]
      attr_reader :consumer
      # @return [Array] order of message removal
      attr_reader :pop_order
      # @return [Symbol] callback method name
      attr_reader :removal_callback

      # Create new consumer queue instance
      #
      # @param consumer [Consumer]
      # @return [self]
      def initialize(consumer, *args)
        opts = args.detect{|x| x.is_a?(Hash)}
        @consumer = consumer
        @removal_callback = opts[:removal_callback]
        @messages = {}
        @pop_order = []
        @cleaner = nil
      end

      # Message container
      #
      # @yieldparam [Hash] messages
      # @return [Hash] messages or block result
      def messages
        if(block_given?)
          yield @messages
        else
          @messages
        end
      end

      # Register a new connection
      #
      # @param connection [Connection]
      # @return [TrueClass]
      def register_connection(connection)
        messages do |collection|
          collection[connection.identifier] = []
        end
        true
      end

      # Remove connection registration and remove all messages
      #
      # @param identifier [String] connection identifier
      # @return [Array<FrameType::Message>] messages queued for deregistered connection
      def deregister_connection(identifier)
        messages do |collection|
          removed = collection.delete(identifier)
          pop_order.delete(identifier)
          removed
        end
      end

      # Push new message into queue
      #
      # @param message [FrameType::Message]
      # @return [self]
      def push(message)
        unless(message.is_a?(FrameType::Message))
          abort TypeError.new "Expecting `FrameType::Message` but received `#{message.class}`!"
        end
        messages do |collection|
          collection[message.connection.identifier] << message
          pop_order << message.connection.identifier
        end
        signal(:new_message)
        self
      end
      alias_method :<<, :push
      alias_method :enq, :push

      # Pop first item off the queue
      #
      # @return [Object]
      def pop
        message = nil
        until(message)
          wait(:new_message) if pop_order.empty?
          messages do |collection|
            key = pop_order.shift
            if(key)
              message = collection[key].shift
              message = validate_message(message)
            end
          end
        end
        message
      end
      alias_method :deq, :pop

      # @return [Integer] number of queued messages
      def size
        messages do |collection|
          collection.values.map(&:size).inject(&:+)
        end
      end

      # Remove duplicate message from queue if possible
      #
      # @param message [FrameType::Message]
      # @return [TrueClass, FalseClass]
      def scrub_duplicate_message(message)
        messages do |collection|
          idx = collection[message.connection.identifier].index do |msg|
            msg.message_id == message.message_id
          end
          if(idx)
            msg = collection[message.connection.identifier].delete_at(idx)
            if(removal_callback)
              consumer.send(removal_callback, [message])
            end
            true
          else
            false
          end
        end
      end

      # Validate message
      def validate_message(message)
        if(message.instance_stamp > message.instance_stamp + (message.connection.endpoint_settings[:msg_timeout] / 1000.0))
          warn "Message exceeded timeout! Discarding. (#{message})"
          if(removal_callback)
            consumer.send(removal_callback, [message])
          end
          nil
        else
          message
        end
      end

    end
  end
end
