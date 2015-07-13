require_relative '../helpers/spec_helper'

describe Krakow do

  before do
    @nsqd = Krakow::Nsqd.new
    @nsqd.run!
  end

  after do
    @nsqd.halt!
  end

  describe Krakow::Connection do

    before do
      host, port = @nsqd.nsqd_tcp_addresses.first.split(':')
      @connection = Krakow::Connection.new(
        :host => host,
        :port => port
      )
      @connection.init!
      wait_for{ @connection.connected? }
    end

    after do
#      @connection.terminate
    end

    it 'should be connected to nsqd' do
      @connection.connected?.must_equal true
    end

    it 'should have an identifier based on configuration' do
      @connection.identifier.must_equal [@connection.host, @connection.port].join('__')
    end

    it 'should have a settings provided by nsqd' do
      @connection.endpoint_settings.wont_be :empty?
    end

    it 'should not transmit non-frametype messages' do
      ->{ @connection.transmit('ohai') }.must_raise TypeError
    end

  end

end
