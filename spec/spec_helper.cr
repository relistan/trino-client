require "spectator"
require "../src/uuid"
require "../src/trino_client"

Spectator.configure do |config|
  config.add_formatter Spectator::Formatting::HTMLFormatter.new
end

FIXTURES_PATH = if __FILE__ == ""
                  File.expand_path(
                    File.join(File.dirname(__FILE__), "fixtures")
                  )
                else
                  File.expand_path(
                    File.join(File.dirname(__FILE__), "..", "fixtures")
                  )
                end

def get_fixture(name)
  File.read(File.join(FIXTURES_PATH, name))
end

