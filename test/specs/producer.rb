describe Krakow do

  describe Krakow::Producer do

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

  end
end
