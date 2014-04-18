module Krakow
  class Distribution
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
            current_ready = ready_for(connection)
            calculate_ready!(connection)
            unless(current_ready == ready_for(connection))
              debug "Redistribution ready setting update for connection #{connection}"
              set_ready_for(connection)
            end
          end
        end
      end

      # Returns if `ideal` is less than 1
      def less_than_ideal?
        ideal < 1
      end

      # Returns next connection to receive RDY count
      def less_than_ideal_ready!
        admit_defeat = false
        connection = nil
        until(connection || (admit_defeat && less_than_ideal_stack.empty?))
          if(less_than_ideal_stack.nil? || less_than_ideal_stack.empty?)
            @less_than_ideal_stack = waiting_connections
            admit_defeat = true
          end
          con = less_than_ideal_stack.pop
          connection = con unless registry_lookup(con)[:backoff_until] > Time.now.to_i
        end
        if(connection)
          registry_lookup(connection)[:ready] = 1
          connection
        end
      end

      # connection:: Connection
      # args:: optional args (:force)
      # Provides customized RDY set when less than ideal to round
      # robin through connections
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

      # connection:: Connection
      # Update connection ready count
      def calculate_ready!(connection)
        registry_info = registry_lookup(connection)
        unless(less_than_ideal?)
          registry_info[:ready] = ideal - registry_info[:in_flight]
          if(registry_info[:ready] < 0 || registry_info[:backoff_until] > Time.now.to_i)
            registry_info[:ready] = 0
            registry_info[:backoff_timer].cancel if registry[:backoff_timer]
            registry_info[:backoff_timer] = after(registry_info[:backoff_until] - Time.now.to_i) do
              calculate_ready!(connection)
              set_ready_for(connection) unless less_than_ideal?
            end
          end
          registry_info[:ready]
        else
          registry_info[:ready] = 0
        end
      end

      # Returns all connections without RDY state
      def waiting_connections
        registry.find_all do |conn_id, info|
          info[:ready] < 1 && info[:in_flight] < 1 && info[:backoff_until] < Time.now.to_i
        end.map{|conn_id, info| info[:connection] if info}.compact
      end

      # Returns all connections with RDY state
      def rdy_connections
        registry.find_all do |conn_id, info|
          info[:ready] > 0
        end.map{|conn_id, info| info[:connection] if info}.compact
      end

      # Force a connection to give up RDY state so next in stack can receive
      def force_unready
        debug 'Forcing a connection into an unready state due to less than ideal state'
        connection = rdy_connections.shuffle.first
        if(connection)
          debug "Stripping RDY state from connection: #{connection}"
          calculate_ready!(connection)
          set_ready_for(connection)
        else
          warn "Failed to locate available connection for RDY aquisition!"
        end
      end

    end
  end
end
