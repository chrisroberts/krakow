require_relative '../../helpers/spec_helper'

describe Krakow do

  before do
    @nsqd = Krakow::Nsqd.new
    @nsqd.run!
  end

  after do
    @nsqd.halt!
  end

  describe 'High Volume - Single Producer / Large payloads' do

    describe 'With delivery confirmation' do

      before do
        @topic = TOPIC_NAME.shuffle.join
        @producer = @nsqd.nsqd_tcp_addresses.map do |addr|
          host, port = addr.split(':')
          Krakow::Producer.new(:host => host, :port => port, :topic => @topic)
        end.first
        wait_for{ @producer.connected? }
      end

      after do
        @producer.terminate
      end

      it 'should produce all payloads' do
        100.times do
          output = TOPIC_NAME.shuffle.join * 1000
          @producer.write(output).response.must_equal 'OK'
        end
      end

    end

    describe 'Without delivery confirmation' do

      before do
        @topic = TOPIC_NAME.shuffle.join
        @producer = @nsqd.nsqd_tcp_addresses.map do |addr|
          host, port = addr.split(':')
          Krakow::Producer.new(:host => host, :port => port, :topic => @topic, :connection_options => {:options => {:response_wait => 0}})
        end.first
        wait_for{ @producer.connected? }
      end

      after do
        @producer.terminate
      end

      it 'should produce all payloads' do
        10000.times do
          output = TOPIC_NAME.shuffle.join * 1000
          @producer.write(output).must_equal true
        end
      end

    end
  end
end
