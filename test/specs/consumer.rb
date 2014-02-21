describe Krakow do

  describe Krakow::Consumer do

    it 'should not have any connections' do
      @consumer.connections.size.must_equal 0
    end
    it 'should have an empty queue' do
      @consumer.queue.size.must_equal 0
    end

    describe 'with active producer' do

      before do
        @producer.write('msg1', 'msg2', 'msg3')
        @inital_wait ||= sleep(0.8) # allow setup (topic creation, discovery, etc)
      end

      it 'should have one connection' do
        @consumer.connections.size.must_equal 1
      end
      it 'should have three messages queued' do
        @consumer.queue.size.must_equal 3
      end
      it 'should properly confirm messages' do
        3.times do
          msg = @consumer.queue.pop
          @consumer.confirm(msg).must_equal true
        end
        sleep(0.5) # pause to let everything settle
        @consumer.queue.must_be :empty?
      end
      it 'should properly requeue messages' do
        2.times do
          @consumer.confirm(@consumer.queue.pop)
        end
        @consumer.queue.size.must_equal 1
        original_msg = @consumer.queue.pop
        @consumer.queue.must_be :empty?
        @consumer.requeue(original_msg).must_equal true
        sleep(0.2)
        @consumer.queue.size.must_equal 1
        req_msg = @consumer.queue.pop
        req_msg.message_id.must_equal original_msg.message_id
      end

    end
  end
end
