require 'krakow'

module Krakow
  # Features that wrap the connection
  module ConnectionFeatures
    autoload :SnappyFrames, 'krakow/connection_features/snappy_frames'
    autoload :Deflate, 'krakow/connection_features/deflate'
    autoload :Ssl, 'krakow/connection_features/ssl'
  end
end
