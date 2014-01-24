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
      [nslookupd].flatten.map do |location|
        uri = URI.parse(location)
        uri.path = '/lookup'
        uri.query = "topic=#{topic}&ts=#{Time.now.to_i}"
        begin
          content = HTTP.with(:accept => 'application/octet-stream').get(uri.to_s)
          unless(content.respond_to?(:to_hash))
            data = MultiJson.load(content.to_s)
          else
            data = content.to_hash
          end
          if(data['data'] && data['data']['producers'])
            data['data']['producers'].map do |producer|
              Hash[*producer.map{|k,v| [k.to_sym, v]}.flatten]
            end
          end
        rescue => e
          nil
        end
      end.compact.flatten(1).uniq
    end

  end
end
