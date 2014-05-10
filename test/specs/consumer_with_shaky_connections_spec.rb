require_relative '../helpers/spec_helper'

describe Krakow::Consumer do

  with_cluster(:nsqlookupd_count => 2, :nsqd_count => 3)

  before do
    @producers = @cluster.nsqd.map { |q| new_producer(q) }

    @expected_messages = %w(a b c d e f g h i j)
    @expected_messages.each_with_index do |m, idx|
      @producers[idx % @producers.length].write(m)
    end

    @consumer = new_consumer(:max_in_flight => 20)
    wait_for { @consumer.connections.length == 3 }
  end

  after do
    @producers.each { |p| p.terminate if p.alive? }
    @consumer.terminate if @consumer.alive?
  end

  it 'should continue processing messages when a queue is down' do

    # shut down the last nsqd
    @producers.last.terminate
    @cluster.nsqd.last.stop

    # make sure there are more messages on each queue than max in flight
    @producers[0].write(*['hay']*15)
    @producers[1].write(*['hay']*15)

    received = Queue.new

    50.times.map do |i|
      Thread.new do
        begin
          msg = @consumer.queue.pop
          received << msg.message
          msg.confirm
        rescue Krakow::Error::ConnectionFailure, Krakow::Error::ConnectionUnavailable
          # ignore
        end
      end
    end

    wait_for(20){ received.size >= 30 }

    received.size.must_be :>=, 30

  end

  it 'should process messages from a new queue when it comes online' do
    begin
      @producers.last.terminate
      nsqd = @cluster.nsqd.last
      nsqd.stop
      sleep(0.1)

      thread = Thread.new do
        nsqd.start
        sleep(1)
        producer = new_producer(nsqd)
        producer.write('needle')
        producer.terminate
      end

      wait_for(20) do
        string = nil
        until(string == 'needle')
          msg = @consumer.queue.pop
          msg.confirm rescue Krakow::Error::ConnectionFailure
          string = msg.message
        end
        true
      end

      thread.join

    end
  end

  it 'should be able to handle all queues going offline and coming back' do
    begin
      expected_messages = ['BACK1', 'BACK2', 'BACK3']

      thread = Thread.new do
        @cluster.nsqd.each { |q| q.stop }
        @cluster.nsqd.each { |q| q.start }

        sleep(0.5)

        @cluster.nsqd.each_with_index do |q, idx|
          p = new_producer(q)
          p.write(expected_messages[idx])
          p.terminate
        end
      end

      Timeout::timeout(20) do
        received_messages = []

        while (expected_messages & received_messages).length < expected_messages.length do
          msg = @consumer.queue.pop
          received_messages << msg.content
          msg.confirm rescue Krakow::Error::ConnectionFailure
        end

        # woohoo we got em all!
      end

    ensure
      thread.join
    end
  end

  it 'should be able to rely on the second nsqlookupd if the first dies' do
    @cluster.nsqlookupd.first.stop

    producer = new_producer(@cluster.nsqd.first, :topic => 'new-topic')
    producer.write('new message on new topic')
    consumer = new_consumer(:topic => 'new-topic')

    Timeout::timeout(5) do
      msg = consumer.queue.pop
      msg.content.must_equal 'new message on new topic'
    end
  end

end
