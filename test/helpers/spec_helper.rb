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
end

Celluloid.logger.level = 1
