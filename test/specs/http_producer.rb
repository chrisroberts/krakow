describe Krakow do

  describe Krakow::Producer::Http do

    before do
      @http = Krakow::Test._http_producer
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
      @consumer.terminate
      @http.write('msg1', 'msg2', 'msg3').status_code.must_equal 200
      et_response = @http.empty_topic
      et_response.status_code.must_equal 200
      et_response.status_txt.must_equal 'OK'
      @consumer = Krakow::Test._consumer
      sleep(0.2)
      @consumer.connections.wont_be :empty?
      @consumer.queue.must_be :empty?
    end
    it 'should empty channel' do
      @consumer.terminate
      @http.write('msg0')
      @http.pause_channel('chan2').status_code.must_equal 200
      consumer1 = Krakow::Test._consumer(:channel => 'chan1')
      consumer2 = Krakow::Test._consumer(:channel => 'chan2')
      @http.write('msg1', 'msg2', 'msg3').status_code.must_equal 200
      sleep(0.5)
      consumer1.queue.size.must_equal 4
      consumer2.queue.size.must_equal 0
      @http.empty_channel('chan2').status_code.must_equal 200
      @http.unpause_channel('chan2').status_code.must_equal 200
      sleep(0.5)
      consumer1.queue.size.must_equal 4
      consumer2.queue.size.must_equal 0
      consumer1.terminate
      consumer2.terminate
    end
    it 'should pause channel' do
      @consumer.terminate
      @http.write('msg0')
      @http.pause_channel('chan2').status_code.must_equal 200
      consumer1 = Krakow::Test._consumer(:channel => 'chan1')
      consumer2 = Krakow::Test._consumer(:channel => 'chan2')
      @http.write('msg1', 'msg2', 'msg3').status_code.must_equal 200
      sleep(0.5)
      consumer1.queue.size.must_equal 4
      consumer2.queue.size.must_equal 0
      consumer1.terminate
      consumer2.terminate
    end
    it 'should unpause channel' do
      @consumer.terminate
      @http.write('msg0')
      @http.pause_channel('chan2').status_code.must_equal 200
      consumer1 = Krakow::Test._consumer(:channel => 'chan1')
      consumer2 = Krakow::Test._consumer(:channel => 'chan2')
      @http.write('msg1', 'msg2', 'msg3').status_code.must_equal 200
      sleep(0.5)
      consumer1.queue.size.must_equal 4
      consumer2.queue.size.must_equal 0
      @http.unpause_channel('chan2').status_code.must_equal 200
      sleep(0.5)
      consumer2.queue.size.must_equal 4
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

end
