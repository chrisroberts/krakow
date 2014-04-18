require 'bundler'
Bundler.setup

require 'timeout'
require 'krakow'
require 'minitest/autorun'
require 'minitest/pride'
require_relative 'cluster_plugin'

class MiniTest::Test
  include ClusterPlugin

  # Block execution until a condition is met
  # Times out after 5 seconds by default
  #
  # example:
  #   wait_for { @consumer.queue.length > 0 }
  #
  def wait_for(timeout = 5, &block)
    Timeout::timeout(timeout) do
      loop do
        break if yield
        sleep(0.01)
      end
    end
  end

  # Assert that something must take longer than a certain amount of time to complete
  def must_take_longer_than(time, &block)
    start_time = Time.now.to_f
    yield
    end_time = Time.now.to_f

    (end_time - start_time).must_be :>, time
  end

  def must_take_less_than(time, &block)
    start_time = Time.now.to_f
    yield
    end_time = Time.now.to_f

    (end_time - start_time).must_be :<, time
  end
end

Celluloid.logger.level = ENV['DEBUG'] ? 0 : 4
