require 'uri'
require 'http'
require 'multi_json'

module Krakow
  class Discovery

    include Utils::Lazy

    def initialize(args={})
      super
      required! :nslookupd
    end

    # topic:: Topic name
    # Return list of end points with given topic name available
    def lookup(topic)
      result = [nslookupd].flatten.map do |location|
        uri = URI.parse(location)
        uri.path = '/lookup'
        uri.query = "topic=#{topic}&ts=#{Time.now.to_i}"
        begin
          debug "Requesting lookup for topic #{topic} - #{uri}"
          content = HTTP.with(:accept => 'application/octet-stream').get(uri.to_s)
          unless(content.respond_to?(:to_hash))
            data = MultiJson.load(content.to_s)
          else
            data = content.to_hash
          end
          debug "Lookup response (#{uri.to_s}): #{data.inspect}"
          if(data['data'] && data['data']['producers'])
            data['data']['producers'].map do |producer|
              Hash[*producer.map{|k,v| [k.to_sym, v]}.flatten]
            end
          end
        rescue => e
          warn "Lookup exception encountered: #{e.class.name} - #{e}"
          nil
        end
      end.compact.flatten(1).uniq
      debug "Discovery lookup result: #{result.inspect}"
      result
    end

  end
end
