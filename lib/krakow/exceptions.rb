module Krakow
  class Error < StandardError

    class BadResponse < Error
      attr_accessor :result
    end

  end
end
