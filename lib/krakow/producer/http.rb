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
        optional :config, :ssl_context
        arguments[:config] ||= {}
        build_ssl_context if ssl_context
        @uri = URI.parse(endpoint)
      end

      def build_ssl_context
        require 'openssl'
        context = OpenSSL::SSL::SSLContext.new
        context.cert = OpenSSL::X509::Certificate.new(File.open(ssl_context[:certificate]))
        context.key = OpenSSL::PKey::RSA.new(File.open(ssl_context[:key]))
        config[:ssl_context] = context
      end

      def send_message(method, path, args={})
        build = uri.dup
        build.path = "/#{path}"
        response = HTTP.send(method, build.to_s, args.merge(config))
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
