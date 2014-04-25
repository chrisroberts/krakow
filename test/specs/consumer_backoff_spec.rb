require_relative '../helpers/spec_helper'

describe Krakow::Consumer, 'with backoff enabled' do

  with_cluster(:nsqlookupd_count => 1, :nsqd_count => 1)

  before do
    @producers = @cluster.nsqd.map { |q| new_producer(q) }
    @producers.first.write(*%w(1 2 3 4 5 6 7 8 9 10))
  end


  after do
    @producers.each { |p| p.terminate if p.alive? }
    @consumer.terminate if @consumer.alive?
  end

  describe 'when backoff_interval is not set' do
    before do
      @consumer = new_consumer(:discovery_interval => 1, :max_in_flight => 1)
      wait_for { @consumer.connections.length == 1 }
    end

    it 'should not backoff on a connection when a message is requeued' do
      @consumer.queue.pop.requeue
      @consumer.queue.pop.requeue

      must_take_less_than(1) do
        @consumer.queue.pop.confirm
      end
    end
  end

  describe 'when backoff_interval is set to 1' do
    before do
      @consumer = new_consumer(:backoff_interval => 1, :discovery_interval => 1, :max_in_flight => 1)
      wait_for { @consumer.connections.length == 1 }
    end

    it 'should temporarily backoff on a connection when a message is requeued' do
      @consumer.queue.pop.requeue
      @consumer.queue.pop.requeue

      must_take_longer_than(2) do
        @consumer.queue.pop.confirm
      end
    end


    it 'should recover if we lose our connection in the middle of a backoff' do
      @consumer.queue.pop.requeue

      wait_for{ @consumer.queue.size > 0 }

      # bounce connection
      @cluster.nsqd.first.stop
      @cluster.nsqd.first.start

      wait_for(5) do
        @consumer.connections.values.find_all(&:connected?).size == 1
      end

      wait_for do
        @consumer.queue.pop.confirm
      end
    end
  end

end
