require "option_parser"
require "../src/trino-client/trino_client"

user = ENV["USER"] || "unknown-user"
hostname = "localhost"
port = 8080

OptionParser.parse do |parser|
  parser.banner = "Usage: query [arguments]"
  parser.on("-u USERNAME", "--user=USERNAME", "Username to send to Trino ('#{user}')") { |u| user = u }
  parser.on("-h HOSTNAME", "--host=HOSTNAME", "Hostname/IP to connect to Trino ('#{hostname}')") { |h| hostname = h }
  parser.on("-p PORT", "--port=PORT", "Port to connect to Trino (#{port})") { |p| post = p }
  parser.on("--help", "Show this help") { puts parser; exit }
end

client = TrinoClient.new("#{hostname}:#{port}", user)
start_time = Time.local
resp = client.query(STDIN.gets_to_end)
elapsed = "elapsed: #{Time.local - start_time}"

puts resp.to_pretty_json
STDERR.puts elapsed
