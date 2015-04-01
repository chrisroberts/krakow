require_relative '../helpers/spec_helper'

describe Krakow do

  before do
    @nsqd = Krakow::Nsqd.new(
      :nsqd_options => {
        'tls-cert' => File.expand_path(File.join(File.dirname(__FILE__), 'ssl_files', 'ssl.pem')),
        'tls-key' => File.expand_path(File.join(File.dirname(__FILE__), 'ssl_files', 'ssl.key'))
      }
    )
    @nsqd.run!
  end

  after do
    @nsqd.halt!
  end

  describe Krakow::ConnectionFeatures do

    describe Krakow::ConnectionFeatures::Ssl do

      before do
        host, port = @nsqd.nsqd_tcp_addresses.first.split(':')
        @connection = Krakow::Connection.new(
          :host => host,
          :port => port,
          :features => {
            :tls_v1 => true
          }
        )
        @connection.init!
        wait_for{ @connection.connected? }
      end

      after do
        @connection.terminate
      end

      it 'should be connected to nsqd' do
        @connection.connected?.must_equal true
      end

    end
  end
end
