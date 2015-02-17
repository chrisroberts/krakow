require 'krakow/version'
require 'celluloid'

if(ENV['DEBUG'])
  Celluloid.task_class = Celluloid::TaskThread
end

require 'celluloid/autostart'
require 'multi_json'

# NSQ client and producer library
module Krakow

  autoload :Command, 'krakow/command'
  autoload :Connection, 'krakow/connection'
  autoload :ConnectionFeatures, 'krakow/connection_features'
  autoload :Consumer, 'krakow/consumer'
  autoload :Discovery, 'krakow/discovery'
  autoload :Distribution, 'krakow/distribution'
  autoload :Error, 'krakow/exceptions'
  autoload :FrameType, 'krakow/frame_type'
  autoload :Ksocket, 'krakow/ksocket'
  autoload :Producer, 'krakow/producer'
  autoload :Utils, 'krakow/utils'

end
