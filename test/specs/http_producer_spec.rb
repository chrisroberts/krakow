require_relative '../helpers/spec_helper'

describe Krakow::Producer::Http do

  let(:topic) { 'hellomoto' }

  with_cluster(:nsqlookupd_count => 1, :nsqd_count => 1)

  before do
    @http = Krakow::Producer::Http.new(
      :endpoint => "http://#{@cluster.nsqd.first.host}:#{@cluster.nsqd.first.http_port}",
      :topic => topic
    )
  end

  def new_consumer(channel)
    super(:topic => topic, :channel => channel)
  end

  it 'should write single messages successfully' do
    response = @http.write('msg')
    Krakow::Command::Pub.ok.must_include response.status_txt
  end

  it 'should write multiple messages successfully' do
    response = @http.write('msg1', 'msg2', 'msg3')
    Krakow::Command::Mpub.ok.must_include response.status_txt
  end

  it 'should create topic' do
    response = @http.create_topic
    response.status_code.must_equal 200
    response.status_txt.must_equal 'OK'
  end

  it 'should delete topic' do
    c_response = @http.create_topic
    c_response.status_code.must_equal 200
    c_response.status_txt.must_equal 'OK'
    d_response = @http.delete_topic
    d_response.status_code.must_equal 200
    d_response.status_txt.must_equal 'OK'
  end

  it 'should create channel' do
    c_response = @http.create_topic
    c_response.status_code.must_equal 200
    c_response.status_txt.must_equal 'OK'
    ch_response = @http.create_channel('fubar')
    ch_response.status_code.must_equal 200
    ch_response.status_txt.must_equal 'OK'
  end

  it 'should delete channel' do
    c_response = @http.create_topic
    c_response.status_code.must_equal 200
    c_response.status_txt.must_equal 'OK'
    ch_response = @http.create_channel('fubar')
    ch_response.status_code.must_equal 200
    ch_response.status_txt.must_equal 'OK'
    dch_response = @http.delete_channel('fubar')
    dch_response.status_code.must_equal 200
    dch_response.status_txt.must_equal 'OK'
  end

  it 'should empty topic' do
    @http.create_topic
    @http.write('msg1', 'msg2', 'msg3').status_code.must_equal 200
    et_response = @http.empty_topic
    et_response.status_code.must_equal 200
    et_response.status_txt.must_equal 'OK'
    consumer = new_consumer('chan1')
    wait_for{ !consumer.connections.empty? }
    consumer.connections.wont_be :empty?
    consumer.queue.size.must_equal 0
    consumer.terminate
  end

  it 'should empty channel' do
    @http.create_topic
    @http.create_channel('chan1')
    @http.create_channel('chan2')
    sleep(0.2)
    @http.write('msg1', 'msg2', 'msg3', 'msg4').status_code.must_equal 200
    sleep(0.5)
    @http.empty_channel('chan2').status_code.must_equal 200
    sleep(0.2)
    consumer2 = new_consumer('chan2')
    consumer1 = new_consumer('chan1')
    wait_for { consumer1.queue.size == 4 }
    consumer1.queue.size.must_equal 4
    consumer2.queue.size.must_equal 0
    consumer1.terminate
    consumer2.terminate
  end

  it 'should pause and unpause channel' do
    @http.create_topic
    @http.create_channel('chan1')
    @http.create_channel('chan2')
    @http.pause_channel('chan2').status_code.must_equal 200
    sleep(0.1)
    @http.write('msg1', 'msg2', 'msg3').status_code.must_equal 200
    consumer1 = new_consumer('chan1')
    consumer2 = new_consumer('chan2')
    wait_for { consumer1.queue.size == 3 }
    consumer1.queue.size.must_equal 3
    consumer2.queue.size.must_equal 0
    @http.unpause_channel('chan2').status_code.must_equal 200
    wait_for{ consumer2.queue.size >= 3 }
    consumer2.queue.size.must_equal 3
    consumer1.terminate
    consumer2.terminate
  end

  it 'should return stats' do
    stat = @http.stats
    stat.status_code.must_equal 200
    stat.data.must_be_kind_of Hash
  end

  it 'should ping' do
    @http.ping.status_code.must_equal 200
  end

  it 'should fetch info' do
    infos = @http.info
    infos.status_code.must_equal 200
    infos.data.must_be_kind_of Hash
  end

end
