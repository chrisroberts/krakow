# Run all the *_spec.rb files in the specs directory
Dir.glob(File.join(File.dirname(__FILE__), 'specs', '**', '*_spec.rb')).each do |path|
  require File.expand_path(path)
end
