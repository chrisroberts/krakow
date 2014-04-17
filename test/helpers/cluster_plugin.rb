#
# This is used by the specs to set up a cluster of nsqd and nsqlookupd processes
# before each spec and tear them down after each spec runs.
#
# Check out nsq-cluster here: https://github.com/wistia/nsq-cluster

require 'nsq-cluster'

module ClusterPlugin

  def self.included(base)
    base.extend ClassMethods
  end


  def before_setup
    @cluster = NsqCluster.new(cluster_options)
    sleep(0.1)
  end


  def after_teardown
    @cluster.destroy
    sleep(0.1)
  end


  def cluster_options
    {}
  end


  def new_consumer(opts = {})
    Krakow::Consumer.new({
      :nslookupd => @cluster.nsqlookupd_http_endpoints,
      :topic => 'some-topic',
      :channel => 'some-channel',
      :discovery_interval => 0.5,
      :discovery_jitter => 0,
      :max_in_flight => defined?(max_in_flight) ? max_in_flight : 10
    }.merge(opts))
  end

  def new_producer(nsqd, opts = {})
    Krakow::Producer.new({
      :host => nsqd.host,
      :port => nsqd.tcp_port,
      :topic => 'some-topic'
    }.merge(opts))
  end


  module ClassMethods
    def with_cluster(opts = {})
      define_method :cluster_options do
        opts
      end
    end
  end

end
