require_relative '../helpers/spec_helper'

describe Krakow::Consumer, 'with backoff enabled' do

  with_cluster(nsqlookupd_count: 1, nsqd_count: 1)

  before do
    @producers = @cluster.nsqd.map { |q| new_producer(q) }
    @producers.first.write(*%w(1 2 3 4 5 6 7 8 9 10))

    @consumer = new_consumer(backoff_interval: 1, discovery_interval: 1, max_in_flight: 1)
    wait_for { @consumer.connections.length == 1 }
  end


  after do
    @producers.each { |p| p.terminate if p.alive? }
    @consumer.terminate if @consumer.alive?
  end


  it 'should temporarily backoff on a connection when a message is requeued' do
    @consumer.distribution.in_backoff_mode.must_equal false

    @consumer.queue.pop.requeue
    @consumer.queue.pop.requeue

    @consumer.distribution.in_backoff_mode.must_equal true

    wait_for do
      @consumer.queue.pop.finish
      !@consumer.distribution.in_backoff_mode
    end
  end


  it 'should recover if we lose our connection in the middle of a backoff' do
    @consumer.queue.pop.requeue

    # bounce connection
    @cluster.nsqd.first.stop
    @cluster.nsqd.first.start

    wait_for do
      @consumer.queue.pop.finish rescue Krakow::Error::ConnectionFailure
      !@consumer.distribution.in_backoff_mode
    end
  end

end


