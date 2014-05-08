require 'http'
require 'uri'
require 'ostruct'

require 'cgi'
# NOTE: Prevents weird "first" run behavior
begin
  require 'json'
rescue LoadError
  # ignore (maybe log?)
end

require 'krakow'

module Krakow
  class Producer

    # HTTP based producer
    class Http

      include Utils::Lazy
      # @!parse include Krakow::Utils::Lazy::InstanceMethods
      # @!parse extend Krakow::Utils::Lazy::ClassMethods

      # Wrapper for HTTP response hash
      class Response < OpenStruct
      end

      attr_reader :uri

      # @!group Attributes

      # @!macro [attach] attribute
      #   @!method $1
      #     @return [$2] the $1 $0
      #   @!method $1?
      #     @return [TrueClass, FalseClass] truthiness of the $1 $0
      attribute :endpoint, String, :required => true
      attribute :topic, String, :required => true
      attribute :config, Hash, :default => ->{ Hash.new }
      attribute :ssl_context, Hash

      # @!endgroup

      def initialize(args={})
        super
        build_ssl_context if ssl_context
        @uri = URI.parse(endpoint)
      end

      # Create a new SSL context
      #
      # @return [OpenSSL::SSL::SSLContext]
      def build_ssl_context
        require 'openssl'
        context = OpenSSL::SSL::SSLContext.new
        context.cert = OpenSSL::X509::Certificate.new(File.open(ssl_context[:certificate]))
        context.key = OpenSSL::PKey::RSA.new(File.open(ssl_context[:key]))
        config[:ssl_context] = context
      end

      # Send a message via HTTP
      #
      # @param method [String, Symbol] HTTP method to use (:get, :put, etc)
      # @param path [String] URI path
      # @param args [Hash] payload hash
      # @return [Response]
      def send_message(method, path, args={})
        build = uri.dup
        build.path = "/#{path}"
        response = HTTP.send(method, build.to_s, args.merge(config))
        begin
          response = MultiJson.load(response.body.to_s)
        rescue MultiJson::LoadError
          response = {'status_code' => response.code, 'status_txt' => response.body.to_s, 'data' => nil}
        end
        Response.new(response)
      end

      # Send messages
      #
      # @param payload [String] message
      # @return [Response]
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

      # Create the topic
      #
      # @return [Response]
      def create_topic
        send_message(:post, :create_topic,
          :params => {:topic => topic}
        )
      end

      # Delete the topic
      #
      # @return [Response]
      def delete_topic
        send_message(:post, :delete_topic,
          :params => {:topic => topic}
        )
      end

      # Create channel on topic
      #
      # @param chan [String] channel name
      # @return [Response]
      def create_channel(chan)
        send_message(:post, :create_channel,
          :params => {
            :topic => topic,
            :channel => chan
          }
        )
      end

      # Delete channel on topic
      #
      # @param chan [String] channel name
      # @return [Response]
      def delete_channel(chan)
        send_message(:post, :delete_channel,
          :params => {
            :topic => topic,
            :channel => chan
          }
        )
      end

      # Remove all messages from topic
      #
      # @return [Response]
      def empty_topic
        send_message(:post, :empty_topic,
          :params => {:topic => topic}
        )
      end

      # Remove all messages from given channel on topic
      #
      # @param chan [String] channel name
      # @return [Response]
      def empty_channel(chan)
        send_message(:post, :empty_channel,
          :params => {
            :topic => topic,
            :channel => chan
          }
        )
      end

      # Pause messages on given channel
      #
      # @param chan [String] channel name
      # @return [Response]
      def pause_channel(chan)
        send_message(:post, :pause_channel,
          :params => {
            :topic => topic,
            :channel => chan
          }
        )
      end

      # Resume messages on a given channel
      #
      # @param chan [String] channel name
      # @return [Response]
      def unpause_channel(chan)
        send_message(:post, :unpause_channel,
          :params => {
            :topic => topic,
            :channel => chan
          }
        )
      end

      # Server stats
      #
      # @param format [String] format of data
      # @return [Response]
      def stats(format='json')
        send_message(:get, :stats,
          :params => {
            :format => format
          }
        )
      end

      # Ping the server
      #
      # @return [Response]
      def ping
        send_message(:get, :ping)
      end

      # Server information
      #
      # @return [Response]
      def info
        send_message(:get, :info)
      end

    end
  end
end
