require 'krakow'
require 'celluloid'
require 'minitest/autorun'

module Krakow
  class Test
    class << self

      def method_missing(*args)
        name = method = args.first.to_s
        if(name.end_with?('?'))
          name = name.tr('?', '')
        end
        val = ENV[name.upcase] || ENV[name]
        if(method.end_with?('?'))
          !!val
        else
          val
        end
      end

      def _topic
        'krakow-test'
      end

      def _scrub_topic!
        _http_producer.delete_topic
      end

      def _http_producer
        @http_producer ||= Krakow::Producer::Http.new(
          :endpoint => 'http://127.0.0.1:4151',
          :topic => _topic
        )
      end

      def _producer(args={})
        Krakow::Producer.new(
          {
            :host => Krakow::Test.nsq_producer_host || '127.0.0.1',
            :port => Krakow::Test.nsq_producer_port || 4150,
            :topic => 'krakow-test'
          }.merge(args)
        )
      end

      def _consumer(args={})
        Krakow::Consumer.new(
          {
            :nslookupd => Krakow::Test.nsq_lookupd || 'http://127.0.0.1:4161',
            :topic => 'krakow-test',
            :channel => 'default',
            :discovery_interval => 0.5,
            :max_in_flight => 20
          }.merge(args)
        )
      end

    end
  end
end

MiniTest::Spec.before do
  Krakow::Test._scrub_topic!
  @consumer = Krakow::Test._consumer
  @producer = Krakow::Test._producer
end

MiniTest::Spec.after do
  @consumer.terminate if @consumer && @consumer.alive?
  @producer.terminate if @producer && @producer.alive?
  Krakow::Test._scrub_topic!
end

unless(Krakow::Test.debug?)
  Celluloid.logger.level = 3
end

Dir.glob(File.join(File.dirname(__FILE__), 'specs', '*.rb')).each do |path|
  require File.expand_path(path)
end
