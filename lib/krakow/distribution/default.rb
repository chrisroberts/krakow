require 'krakow'

module Krakow
  class Distribution
    # Default distribution implementation. This uses a round-robin
    # approach for less than ideal states.
    class Default < Distribution

      attr_reader :less_than_ideal_stack, :watch_dog

      # recalculate `ideal` and update RDY on connections
      def redistribute!
        @ideal = registry.size < 1 ? 0 : max_in_flight / registry.size
        debug "Distribution calculated ideal: #{ideal}"
        if(less_than_ideal?)
          registry.each do |connection_id, reg_info|
            reg_info[:ready] = 0
          end
          max_in_flight.times do
            less_than_ideal_ready!
          end
          connections.each do |connection|
            set_ready_for(connection, :force)
          end
          watch_dog.cancel if watch_dog
          @watch_dog = every(watch_dog_interval) do
            force_unready
          end
        else
          if(watch_dog)
            watch_dog.cancel
            @watch_dog = nil
          end
          connections.each do |connection|
            current_ready = ready_for(connection.identifier)
            calculate_ready!(connection.identifier)
            unless(current_ready == ready_for(connection.identifier))
              debug "Redistribution ready setting update for connection #{connection}"
              set_ready_for(connection)
            end
          end
        end
      end

      # Is ideal less than 1
      #
      # @return [TrueClass, FalseClass]
      def less_than_ideal?
        ideal < 1
      end

      # Find next connection to receive RDY count
      #
      # @return [Krakow::Connection, nil]
      def less_than_ideal_ready!
        admit_defeat = false
        connection = nil
        until(connection || (admit_defeat && less_than_ideal_stack.empty?))
          if(less_than_ideal_stack.nil? || less_than_ideal_stack.empty?)
            @less_than_ideal_stack = waiting_connections
            admit_defeat = true
          end
          con = less_than_ideal_stack.pop
          if(con)
            unless(registry_lookup(con.identifier)[:backoff_until] > Time.now.to_i)
              connection = con
            end
          end
        end
        if(connection)
          registry_lookup(connection.identifier)[:ready] = 1
          connection
        end
      end

      # Adds extra functionality to provide round robin RDY setting
      # when in less than ideal state
      #
      # @param connection [Krakow::Connection]
      # @param args [Symbol]
      # @return [Krakow::FrameType::Error, nil]
      def set_ready_for(connection, *args)
        super connection
        if(less_than_ideal? && !args.include?(:force))
          debug "RDY set ignored due to less than ideal state (con: #{connection})"
          con = less_than_ideal_ready!
          if(con)
            watch_dog.reset if watch_dog
            super con
          else
            warn 'Failed to set RDY state while less than ideal. Connection stack is empty!'
          end
        end
      end

      # Update connection ready count
      # @param connection_identifier [String]
      # @return [Integer, nil]
      def calculate_ready!(connection_identifier)
        begin
          registry_info = registry_lookup(connection_identifier)
          unless(less_than_ideal?)
            registry_info[:ready] = ideal - registry_info[:in_flight]
            if(registry_info[:ready] < 0 || registry_info[:backoff_until] > Time.now.to_i)
              registry_info[:ready] = 0
              registry_info[:backoff_timer].cancel if registry[:backoff_timer]
              registry_info[:backoff_timer] = after(registry_info[:backoff_until] - Time.now.to_i) do
                calculate_ready!(connection_identifier)
                set_ready_for(connection_lookup(connection_identifier)) unless less_than_ideal?
              end
            end
            registry_info[:ready]
          else
            registry_info[:ready] = 0
          end
        rescue Error::ConnectionFailure
          warn 'Failed connection encountered!'
        rescue Error::ConnectionUnavailable
          warn 'Unavailable connection encountered!'
        end
      end

      # All connections without RDY state
      #
      # @return [Array<Krakow::Connection>]
      def waiting_connections
        registry.find_all do |conn_id, info|
          info[:ready] < 1 && info[:in_flight] < 1 && info[:backoff_until] < Time.now.to_i
        end.map{|conn_id, info| connection_lookup(conn_id) }.compact
      end

      # All connections with RDY state
      #
      # @return [Array<Krakow::Connection>]
      def rdy_connections
        registry.find_all do |conn_id, info|
          info[:ready] > 0
        end.map{|conn_id, info| connection_lookup(conn_id) }.compact
      end

      # Force a connection to give up RDY state so next in stack can receive
      #
      # @return [nil]
      def force_unready
        debug 'Forcing a connection into an unready state due to less than ideal state'
        connection = rdy_connections.shuffle.first
        if(connection)
          debug "Stripping RDY state from connection: #{connection}"
          calculate_ready!(connection.identifier)
          set_ready_for(connection)
        else
          warn "Failed to locate available connection for RDY aquisition!"
        end
        nil
      end

    end
  end
end
