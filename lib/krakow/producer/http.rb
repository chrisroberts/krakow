require 'http'
require 'uri'
require 'ostruct'

module Krakow
  class Producer
    class Http

      class Response < OpenStruct
      end

      include Utils::Lazy

      attr_reader :uri

      def initialize(args={})
        super
        required! :endpoint, :topic
        @uri = URI.parse(endpoint)
      end

      def send_message(method, path, args={})
        build = uri.dup
        build.path = "/#{path}"
        response = HTTP.send(method, build.to_s, args)
        begin
          response = MultiJson.load(response.response.body)
        rescue MultiJson::LoadError
          response = {'status_code' => response == 'OK' ? 200 : nil, 'status_txt' => response, 'data' => nil}
        end
        Response.new(response)
      end

      def write(*payload)
        if(payload.size == 1)
          payload = payload.first
          send_message(:post, :pub,
            :body => payload,
            :params => {:topic => topic}
          )
        else
          send_message(:post, :mpub,
            :body => payload.join("\n"),
            :params => {:topic => topic}
          )
        end
      end

      def create_topic
        send_message(:post, :create_topic,
          :params => {:topic => topic}
        )
      end

      def delete_topic
        send_message(:post, :delete_topic,
          :params => {:topic => topic}
        )
      end

      def create_channel(chan)
        send_message(:post, :create_channel,
          :params => {
            :topic => topic,
            :channel => chan
          }
        )
      end

      def delete_channel(chan)
        send_message(:post, :delete_channel,
          :params => {
            :topic => topic,
            :channel => chan
          }
        )
      end

      def empty_topic
        send_message(:post, :empty_topic,
          :params => {:topic => topic}
        )
      end

      def empty_channel(chan)
        send_message(:post, :empty_channel,
          :params => {
            :topic => topic,
            :channel => chan
          }
        )
      end

      def pause_channel(chan)
        send_message(:post, :pause_channel,
          :params => {
            :topic => topic,
            :channel => chan
          }
        )
      end

      def unpause_channel(chan)
        send_message(:post, :unpause_channel,
          :params => {
            :topic => topic,
            :channel => chan
          }
        )
      end

      def stats(format='json')
        send_message(:get, :stats,
          :params => {
            :format => format
          }
        )
      end

      def ping
        send_message(:get, :ping)
      end

      def info
        send_message(:get, :info)
      end

    end
  end
end
