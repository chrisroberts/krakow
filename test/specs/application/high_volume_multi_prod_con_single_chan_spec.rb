require_relative '../../helpers/spec_helper'

describe Krakow do

  before do
    @nsqd = Krakow::Nsqd.new(:nsqds => 20)
    @nsqd.run!
  end

  after do
    @nsqd.halt!
  end

  describe 'High Volume - Multi Producer / Multi Consumer / Single Channel' do

    before do
      @topic = TOPIC_NAME.shuffle.join
      @producers = @nsqd.nsqd_tcp_addresses.map do |addr|
        host, port = addr.split(':')
        Krakow::Producer.new(:host => host, :port => port, :topic => @topic)
      end.map do |producer|
        producer.write('seed')
        producer
      end
      @consumers = 15.times.map do
        Krakow::Consumer.new(:nsqlookupd => @nsqd.lookupd_http_addresses.first, :topic => @topic, :channel => 'default', :max_in_flight => 20)
      end
      wait_for(10){ @consumers.all?{|consumer| !consumer.connections.empty?} }
      @consumers.each do |consumer|
        if(consumer.queue.size > 0)
          consumer.queue.pop.confirm
        end
      end
    end

    after do
      @producers.map(&:terminate)
      @consumers.map(&:terminate)
    end

    it 'should consume all generated messages' do
      @generated = []
      @received = Queue.new
      generator = lambda do
        1000.times do |i|
          @generated << i.to_s
          @producers.shuffle.first.write(i.to_s)
        end
      end
      consumers = @consumers.map do |consumer|
        Thread.new do
          loop do
            msg = consumer.queue.pop
            consumer.confirm(msg)
            @received.push(msg.message)
          end
        end
      end
      Thread.new{ generator.call }
      collector = Thread.new do
        loop do
          @generated.delete(@received.pop)
        end
      end
      sleep(3)
      wait_for(120){@received.size == 0 && @generated.empty?}
      @received.must_be :empty?
      @generated.must_be :empty?
    end

  end

end
