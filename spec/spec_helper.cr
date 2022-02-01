require "spectator"
require "../src/trino_client"

Spectator.configure do |config|
  config.add_formatter Spectator::Formatting::HTMLFormatter.new
end
