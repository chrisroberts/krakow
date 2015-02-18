require 'childprocess'

ChildProcess.posix_spawn = true

require 'tmpdir'
require 'fileutils'
require_relative 'spec_helper'

module Krakow
  class Nsqd

    def self.ports(n=1)
      @port ||= 6677
      result = n.times.inject([]) do |m,k|
        m.push(@port += 1)
      end
      n == 1 ? result.first : result
    end

    attr_reader :args
    attr_reader :nsqds
    attr_reader :lookupds

    def initialize(args={})
      @args = args
      @nsqds = []
      @lookupds = []
      @dir = Dir.mktmpdir('krakow')
    end

    def run!
      @lookupds = args.fetch(:lookupds, 1).times.map do
        build_lookupd
      end
      @nsqds = args.fetch(:nsqds, 1).times.map do
        build_nsqd
      end
      nsqd_tcp_addresses.each do |addr|
        begin
          TCPSocket.new(*addr.split(':'))
        rescue Errno::ECONNREFUSED
          retry
        end
      end
    end

    def halt!
      @nsqds.map(&:first).map(&:stop)
      @lookupds.map(&:first).map(&:stop)
      @nsqds.map(&:first).map(&:wait)
      @lookupds.map(&:first).map(&:wait)
      FileUtils.rm_rf(@dir)
    end

    def build_nsqd
      ports = self.class.ports(2)
      options = {
        'data-path' => @dir,
        'tcp-address' => "0.0.0.0:#{ports.first}",
        'http-address' => "0.0.0.0:#{ports.last}"
      }.merge(args.fetch(:nsqd_options, {}))
      cmd = ['nsqd'] + options.map{|k,v| ["-#{k}", v]}
      unless(@lookupds.empty?)
        lookupd_tcp_addresses.each do |laddr|
          cmd.push('-lookupd-tcp-address').push(laddr)
        end
      end
      [start_process(ChildProcess.build(*cmd.flatten)), options]
    end

    def build_lookupd
      ports = self.class.ports(2)
      options = {
        'tcp-address' => "0.0.0.0:#{ports.first}",
        'http-address' => "0.0.0.0:#{ports.last}"
      }.merge(args.fetch(:lookupd_options, {}))
      cmd = ['nsqlookupd'] + options.map{|k,v| ["-#{k}", v]}
      [start_process(ChildProcess.build(*cmd.flatten)), options]
    end

    def nsqd_tcp_addresses
      @nsqds.map(&:last).map do |nopts|
        nopts['tcp-address']
      end
    end

    def nsqd_http_addresses
      @nsqds.map(&:last).map do |nopts|
        "http://#{nopts['http-address']}"
      end
    end

    def lookupd_http_addresses
      @lookupds.map(&:last).map do |lopts|
        "http://#{lopts['http-address']}"
      end
    end

    def lookupd_tcp_addresses
      @lookupds.map(&:last).map do |lopts|
        lopts['tcp-address']
      end
    end

    def start_process(process)
      if(ENV['DEBUG'])
        process.io.inherit!
      end
      process.cwd = @dir
      process.start
      process
    end

  end
end
