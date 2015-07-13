require_relative '../helpers/spec_helper'

describe Krakow do

  before do
    @nsqd = Krakow::Nsqd.new
    @nsqd.run!
  end

  after do
    @nsqd.halt!
  end

  describe Krakow::Producer do

    describe 'State' do
      it 'should teardown all resources on terminate' do
        topic = TOPIC_NAME.shuffle.join
        host, port = @nsqd.nsqd_tcp_addresses.first.split(':')
        producer = Krakow::Producer.new(
          :host => host,
          :port => port,
          :topic => @topic
        )
        producer.connected?.must_equal true
        connection = producer.connection
        socket = connection.socket
        producer.terminate
        wait_for{ !producer.alive? }
        producer.alive?.must_equal false
        wait_for{ !connection.alive? }
        connection.alive?.must_equal false
        socket.alive?.must_equal false
      end
    end

    describe 'Usage' do
      before do
        @topic = TOPIC_NAME.shuffle.join
        host, port = @nsqd.nsqd_tcp_addresses.first.split(':')
        @producer = Krakow::Producer.new(
          :host => host,
          :port => port,
          :topic => @topic
        )
      end

      after do
        @producer.terminate
      end

      it 'should have an active connection' do
        @producer.connected?.must_equal true
      end

      it 'should write a single message' do
        @producer.write('testing').response.must_equal 'OK'
      end

      it 'should write multiple messages' do
        @producer.write('testing1', 'testing2', 'testing3').response.must_equal 'OK'
      end

      it 'should automatically reconnect a failed connection' do
        @producer.connection.socket.socket.close
        result = nil
        begin
          @producer.write('testing')
        rescue => result
        end
        [IOError, Krakow::Error::ConnectionUnavailable].must_include result.class
        sleep(0.1)
        wait_for{ @producer.connected? }
        @producer.write('testing').response.must_equal 'OK'
      end

    end

  end

  describe Krakow::Producer::Http do
    before do
      @producer = Krakow::Producer::Http.new(
        :endpoint => @nsqd.nsqd_http_addresses.first,
        :topic => TOPIC_NAME.shuffle.join
      )
    end

    it 'should write a single message' do
      result = @producer.write('testing')
      result.status_txt.must_equal 'OK'
      result.response.must_equal 'OK'
    end

    it 'should write multiple messages' do
      result = @producer.write('testing1', 'testing2', 'testing3')
      result.status_txt.must_equal 'OK'
      result.response.must_equal 'OK'
    end

  end

end
