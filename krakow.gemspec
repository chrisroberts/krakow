$LOAD_PATH.unshift File.expand_path(File.dirname(__FILE__)) + '/lib/'
require 'krakow/version'
Gem::Specification.new do |s|
  s.name = 'krakow'
  s.version = Krakow::VERSION.version
  s.summary = 'NSQ library'
  s.author = 'Chris Roberts'
  s.email = 'code@chrisroberts.org'
  s.homepage = 'http://github.com/chrisroberts/krakow'
  s.description = 'NSQ ruby library'
  s.license = 'Apache 2.0'
  s.require_path = 'lib'
  s.add_dependency 'celluloid-io'
  s.add_dependency 'http'
  s.add_dependency 'multi_json'
  s.add_dependency 'digest-crc'
  s.files = Dir['lib/**/*'] + %w(krakow.gemspec README.md CHANGELOG.md CONTRIBUTING.md LICENSE)
  s.extra_rdoc_files = %w(CHANGELOG.md CONTRIBUTING.md LICENSE)
end
