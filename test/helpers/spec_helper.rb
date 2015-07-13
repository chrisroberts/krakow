require 'bundler'
Bundler.setup

require 'timeout'
require 'krakow'
require 'minitest/autorun'
require_relative 'nsqd'

TOPIC_NAME = ('a'..'z').to_a + ('A'..'Z').to_a

class MiniTest::Test

  # Block execution until a condition is met
  # Times out after 5 seconds by default
  #
  # example:
  #   wait_for { @consumer.queue.length > 0 }
  #
  def wait_for(timeout = 5)
    raise ArgumentError.new 'Block must be provided!' unless block_given?
    Timeout::timeout(timeout) do
      loop do
        break if yield
        sleep(0.1)
      end
    end
  end

  # Assert that something must take longer than a certain amount of time to complete
  def must_take_longer_than(time, roof=false, &block)
    start_time = Time.now.to_f
    yield
    end_time = Time.now.to_f

    result = (end_time - start_time)
    result = result.ceil if roof
    result.must_be :>, time
  end

  def must_take_less_than(time, roof=false, &block)
    start_time = Time.now.to_f
    yield
    end_time = Time.now.to_f

    result = (end_time - start_time)
    result = result.ceil if roof
    result.must_be :<, time
  end
end
