require 'securerandom'
require_relative '../../helpers/spec_helper'

describe Krakow do

  before do
    @nsqd = Krakow::Nsqd.new
    @nsqd.run!
  end

  after do
    @nsqd.halt!
  end

  describe 'Large mpub from producer with sufficient wait response' do

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
              :response_wait => 10
            }
          }
        )
      end.first
      wait_for{ @producer.connected? }
    end

    after do
      @producer.terminate
    end

    it 'should deliver large mpub payload and receive result' do
      msgs = 100000.times.map{ SecureRandom.hex }
      msgs.each_slice(50000) do |slice|
        result = @producer.write(*slice)
        result.must_be :is_a?, Krakow::FrameType
      end
    end

  end

  describe 'Large mpub from producer without sufficient wait response' do

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
              :response_wait => 0.01
            }
          }
        )
      end.first
      wait_for{ @producer.connected? }
    end

    after do
      @producer.terminate
    end

    it 'should deliver large mpub payload and receive result' do
      msgs = 100000.times.map{ SecureRandom.hex }
      lambda do
        msgs.each_slice(50000) do |slice|
          result = @producer.write(*slice)
        end
      end.must_raise Krakow::Error::BadResponse::NoResponse
    end

  end

end
