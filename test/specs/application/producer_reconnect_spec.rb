require_relative '../../helpers/spec_helper'

describe Krakow do

  before do
    @nsqd = Krakow::Nsqd.new
    @nsqd.run!
  end

  after do
    @nsqd.halt!
  end

  describe 'Producer with socket failures' do

    before do
      @topic = TOPIC_NAME.shuffle.join
      @producer = @nsqd.nsqd_tcp_addresses.map do |addr|
        host, port = addr.split(':')
        Krakow::Producer.new(
          :host => host,
          :port => port,
          :topic => @topic,
          :connection_options => {
            :options => {
              :response_wait => 0
            }
          }
        )
      end.first
      wait_for{ @producer.connected? }
    end

    after do
      @producer.terminate
    end

    it 'should send all messages' do
      result = []
      100.times do |i|
        begin
          result << @producer.write('test')
          if(i % 20 == 0)
            @producer.connection.socket.socket.close
          end
        rescue
          wait_for{ @producer.connected? }
          retry
        end
      end
      result.size.must_equal 100
    end

  end
end
