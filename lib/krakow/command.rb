module Krakow
  class Command

    include Utils::Lazy

    # Return command name
    def name
      self.class.name.split('::').last.upcase
    end

    # Convert to line output
    def to_line
      raise NoMethodError.new 'No line conversion method defined!'
    end

    # Make all the commands available
    Dir.glob(File.join(File.dirname(__FILE__), 'command', '*')).each do |path|
      autoload(
        File.basename(path).sub(File.extname(path), '').capitalize.to_sym,
        File.join('krakow/command', File.basename(path).sub(File.extname(path), ''))
      )
    end

  end
end
