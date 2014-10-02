require_relative '../helpers/spec_helper'

describe Krakow::Consumer do

  let(:max_in_flight) { 10 }
  let(:consumer) { new_consumer }

  before do
    @producers = @cluster.nsqd.map { |q| new_producer(q) }
  end

  after do
    if(@producers)
      @producers.each { |p| p.terminate if p.alive? }
    end
    consumer.terminate
  end

  describe 'with no active producers' do
    with_cluster(:nsqlookupd_count => 1)

    it 'should have an empty queue and not have any connections' do
      consumer
      sleep(0.2)
      consumer.queue.size.must_equal 0
      consumer.connections.size.must_equal 0
    end
  end


  describe 'with one active producer' do
    before do
      @producers.first.write('message')
      consumer
      wait_for { consumer.queue.length > 0 }
    end

    describe 'with a high msg_timeout for nsqd' do
      with_cluster(:nsqlookupd_count => 1, :nsqd_count => 1, :nsqd_options => { :msg_timeout => '60s' })

      it 'should have one connection' do
        consumer.connections.size.must_equal 1
      end

      it 'should have a message queued' do
        consumer.queue.size.must_equal 1
      end

      it 'should properly requeue messages' do
        original_msg = consumer.queue.pop
        original_msg.attempts.must_equal 1
        original_msg.requeue
        Timeout::timeout(1) do
          req_msg = consumer.queue.pop
          req_msg.message_id.must_equal original_msg.message_id
          req_msg.attempts.must_equal 2
        end
      end

      it 'should properly requeue messages via message_id' do
        original_msg = consumer.queue.pop
        original_msg.attempts.must_equal 1
        consumer.requeue(original_msg.message_id)
        Timeout::timeout(1) do
          req_msg = consumer.queue.pop
          req_msg.message_id.must_equal original_msg.message_id
          req_msg.attempts.must_equal 2
        end
      end
    end

    describe 'with a low msg_timeout for nsqd' do
      let(:msg_timeout) { 1 }
      with_cluster(:nsqlookupd_count => 1, :nsqd_count => 1, :nsqd_options => { :msg_timeout => '1s' })

      it 'should properly confirm messages' do
        consumer.queue.length.must_equal 1
        consumer.queue.pop.confirm
        sleep(msg_timeout * 1.1) # pause for slightly longer than msg_timeout
        consumer.queue.must_be :empty?
      end

      it 'should confirm messages via message_id' do
        consumer.queue.length.must_equal 1
        msg = consumer.queue.pop
        consumer.confirm(msg.message_id)
        sleep(msg_timeout * 1.1) # pause for slightly longer than msg_timeout
        consumer.queue.must_be :empty?
      end

      it 'should receive the same message twice if we fail to process it before it times out' do
        original_msg = consumer.queue.pop
        new_msg = consumer.queue.pop
        new_msg.message_id.must_equal original_msg.message_id
        new_msg.attempts.must_equal 2
      end

      it 'should properly touch messages' do
        original_msg = consumer.queue.pop
        original_msg.touch
        sleep(msg_timeout * 0.8)
        original_msg.touch
        sleep(msg_timeout * 0.8)
        original_msg.confirm
        sleep(msg_timeout * 1.1)
        consumer.queue.must_be :empty?
      end

    end
  end

  describe 'where there are more messages on the queue than fit in flight' do
    with_cluster(:nsqlookupd_count => 1, :nsqd_count => 1)
    let(:max_in_flight) { 1 }

    it 'should process all the messages' do
      expected_messages = %w(a b c d e f g h i j)
      @producers.first.write(*expected_messages)
      received_messages = []
      expected_messages.length.times do
        msg = consumer.queue.pop
        received_messages << msg.message
        msg.confirm
      end
      received_messages.sort.must_equal(expected_messages)
    end

  end


  describe 'when max_in_flight < num_connections' do
    let(:max_in_flight) { 1 }
    with_cluster(:nsqlookupd_count => 1, :nsqd_count => 3)


    it 'should be able to properly get all messages from all nsqds' do
      expected_messages = %w(a b c d e f g h i j)
      expected_messages.each_with_index do |m, idx|
        @producers[idx % @producers.length].write(m)
      end

      wait_for { consumer.connections.length == 3 }

      messages = []

      expected_messages.length.times do
        wait_for(5) do
          msg = consumer.queue.pop
          messages << msg.message
          msg.confirm
          true
        end
      end
      messages.sort.must_equal expected_messages
      consumer.queue.must_be :empty?
    end

    it 'should get messages from all queues even if the first queue it reads from has a never ending supply of messages' do
      begin
        # produce a never ending stream of messages on the first queue
        thread = Thread.new do
          loop do
            @producers.first.write('never ending')
            sleep(0.1)
          end
        end

        consumer = new_consumer(:redistribution_interval => 1)

        # wait for the consumer to glom onto that never ending stream
        wait_for do
          msg = consumer.queue.pop
          msg.confirm
          msg.content == 'never ending'
        end

        @producers.last.write('a new hope')

        wait_for do
          msg = consumer.queue.pop
          msg.confirm
          msg.content == 'a new hope'
        end

        # we did it!
      ensure
        thread.kill
      end
    end

  end


  describe 'when max_in_flight >= num_connections' do
    let(:max_in_flight) { 10 }
    with_cluster(:nsqlookupd_count => 1, :nsqd_count => 5)

    before do
      @expected_messages = (1..100).to_a.map(&:to_s).sort
      @expected_messages.each_with_index do |m, idx|
        @producers[idx % @producers.length].write(m)
      end

      wait_for(20) { consumer.connections.length == 5 }
    end

    it 'should be able to properly get all messages from all nsqds' do
      messages = []
      Timeout::timeout(10) do
        @expected_messages.length.times do
          msg = consumer.queue.pop
          messages << msg.message
          msg.confirm
        end
      end
      messages.sort.must_equal @expected_messages.sort
      consumer.queue.must_be :empty?
    end
  end


  describe 'two consumers on one queue' do
    with_cluster(:nsqlookupd_count => 1, :nsqd_count => 1)

    it 'should get all the messages between the two of them' do
      expected_messages = (1..100).to_a.map(&:to_s).sort
      @producers.first.write(*expected_messages)

      threads = []
      receive_queue = Queue.new

      begin
        # fire up 5 consumers
        5.times do
          threads << Thread.new do
            consumer = new_consumer
            loop do
              msg = consumer.queue.pop
              receive_queue.push(msg.message)
              msg.confirm
            end
          end
        end

        wait_for(10){ receive_queue.length == expected_messages.length }
      ensure
        threads.each(&:terminate)
        threads.each(&:join)
      end

      # turn the queue into an array so we can compare it
      received_messages = []
      receive_queue.length.times do
        received_messages << receive_queue.pop(true)
      end

      received_messages.sort.must_equal expected_messages
    end
  end
end
