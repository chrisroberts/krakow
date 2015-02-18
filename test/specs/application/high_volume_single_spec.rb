require_relative '../../helpers/spec_helper'

describe Krakow do

  before do
    @nsqd = Krakow::Nsqd.new
    @nsqd.run!
  end

  after do
    @nsqd.halt!
  end

  describe 'High Volume - Single Producer / Single Consumer / Single Channel' do

    before do
      @topic = TOPIC_NAME.shuffle.join
      @producer = @nsqd.nsqd_tcp_addresses.map do |addr|
        host, port = addr.split(':')
        Krakow::Producer.new(:host => host, :port => port, :topic => @topic)
      end.map do |producer|
        producer.write('seed')
        producer
      end.first
      sleep(1)
      @consumer = Krakow::Consumer.new(
        :nsqlookupd => @nsqd.lookupd_http_addresses.first,
        :topic => @topic, :channel => 'default', :max_in_flight => 100
      )
      wait_for(10){ !@consumer.connections.empty? }
      @consumer.queue.pop.confirm
    end

    after do
      @producer.terminate
      @consumer.terminate
    end

    it 'should consume all generated messages' do
      @generated = []
      @received = Queue.new
      generator = lambda do
        1000.times do |i|
          @generated << i.to_s
          @producer.write(i.to_s)
        end
      end
      Thread.new do
        loop do
          msg = @consumer.queue.pop
          @consumer.confirm(msg)
          @received.push(msg.message)
        end
      end
      Thread.new{ generator.call }
      collector = Thread.new do
        loop do
          @generated.delete(@received.pop)
        end
      end
      sleep(3)
      start = Time.now.to_i
      wait_for(120){@received.size == 0 && @generated.empty?}
      @received.must_be :empty?
      @generated.must_be :empty?
    end

  end
end
