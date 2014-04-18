require_relative '../helpers/spec_helper'

describe Krakow::Producer do

  with_cluster(:nsqd_count => 1)

  before do
    @producer = Krakow::Producer.new(
      :host => @cluster.nsqd.first.host,
      :port => @cluster.nsqd.first.tcp_port,
      :topic => 'hella-good',
      :reconnect_interval => 1
    )
  end

  after do
    @producer.terminate
  end

  it 'should be connected' do
    @producer.connected?.must_equal true
  end

  it 'should write single messages successfully' do
    response = @producer.write('msg')
    response.must_be_kind_of Krakow::FrameType::Response
    Krakow::Command::Pub.ok.must_include response.content
  end

  it 'should write multiple messages successfully' do
    response = @producer.write('msg1', 'msg2', 'msg3')
    response.must_be_kind_of Krakow::FrameType::Response
    Krakow::Command::Mpub.ok.must_include response.content
  end

  it 'should successfully write a single non-string message' do
    response = @producer.write(1)
    response.must_be_kind_of Krakow::FrameType::Response
    Krakow::Command::Pub.ok.must_include response.content
  end

  it 'should successfully write multiple non-string messages' do
    response = @producer.write(1,2,3)
    response.must_be_kind_of Krakow::FrameType::Response
    Krakow::Command::Mpub.ok.must_include response.content
  end

  it 'should raise connection errors when trying to write and the connection is not available' do
    @cluster.nsqd.first.stop
    @cluster.nsqd.first.start

    proc {
      @producer.write('hi')
    }.must_raise Krakow::Error::ConnectionUnavailable

    @producer.alive?.must_equal true
  end

  it 'should be able to reconnect automatically' do
    @cluster.nsqd.first.stop
    sleep(0.2)
    @producer.connected?.must_equal false
    @cluster.nsqd.first.start
    sleep(1)
    @producer.connected?.must_equal true

    response = @producer.write('msg1', 'msg2', 'msg3')
    response.must_be_kind_of Krakow::FrameType::Response
    Krakow::Command::Mpub.ok.must_include response.content
  end

end
