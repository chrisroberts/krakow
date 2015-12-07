require_relative '../../helpers/spec_helper'

describe Krakow do

  before do
    @nsqd = Krakow::Nsqd.new(
      :nsqd_options => {
        'msg-timeout' => '5s'
      }
    )
    @nsqd.run!
  end

  after do
    @nsqd.halt!
  end

  describe 'Consumer with socket failures' do

    before do
      @topic = TOPIC_NAME.shuffle.join
      @producer = @nsqd.nsqd_tcp_addresses.map do |addr|
        host, port = addr.split(':')
        Krakow::Producer.new(
          :host => host,
          :port => port,
          :topic => @topic
        )
      end.first
      wait_for{ @producer.connected? }
      100.times do |i|
        @producer.write(i.to_s)
      end
      @consumer = Krakow::Consumer.new(
        :nsqlookupd => @nsqd.lookupd_http_addresses.first,
        :topic => @topic,
        :channel => 'default',
        :max_in_flight => 20,
        :discovery_interval => 5
      )
      @consumer.start!
      wait_for{ @consumer.connected? }
      sleep(1)
    end

    after do
      @producer.terminate
      @consumer.terminate
    end

    it 'should confirm all messages' do
      retries = 0
      result = []
      100.times do |i|
        begin
          wait_for(15){ result << @consumer.queue.pop.confirm }
          if(((i + 1) % 50) == 0)
            @consumer.connections.values.first.socket.socket.close
          end
        rescue => e
          wait_for(30){ @consumer.connected? }
          retries += 1
          raise if retries > 5
          retry
        end
      end
      result.size.must_equal 100
    end

  end
end
