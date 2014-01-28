module Krakow
  class Distribution
    class Default < Distribution

      attr_reader :less_than_ideal_stack, :watch_dog

      # recalculate `ideal` and update RDY on connections
      def redistribute!
        @ideal = max_in_flight / registry.size
        debug "Distribution calculated ideal: #{ideal}"
        if(less_than_ideal?)
          registry.each do |connection, reg_info|
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
        if(less_than_ideal_stack.nil? || less_than_ideal_stack.empty?)
          @less_than_ideal_stack = waiting_connections
        end
        connection = less_than_ideal_stack.pop
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
        if(less_than_ideal?)
          if(args.include?(:force))
            super connection
          else
            debug "RDY set ignored due to less than ideal state (con: #{connection})"
            con = less_than_ideal_ready!
            if(con)
              watch_dog.reset if watch_dog
              super con
            else
              warn 'Failed to set RDY state while less than ideal. Connection stack is empty!'
            end
          end
        else
          super connection
        end
      end

      # connection:: Connection
      # Update connection ready count
      def calculate_ready!(connection)
        registry_info = registry_lookup(connection)
        unless(less_than_ideal?)
          registry_info[:ready] = ideal - registry_info[:in_flight]
          registry_info[:ready] = 0 if registry_info[:ready] < 0
          registry_info[:ready]
        else
          registry_info[:ready] = 0
        end
      end

      # Returns all connections without RDY state
      def waiting_connections
        registry.find_all do |connection, info|
          info[:ready] < 1 && info[:in_flight] < 1
        end.map(&:first).compact
      end

      # Returns all connections with RDY state
      def rdy_connections
        registry.find_all do |connection, info|
          info[:ready] > 0
        end.map(&:first).compact
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
