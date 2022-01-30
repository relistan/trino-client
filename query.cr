require "option_parser"
require "./src/trino-client/trino_client"

user = (ENV["USER"] || "unknown-user")
hostname = "localhost"
port = 8080
password = nil

OptionParser.parse do |parser|
  parser.banner = "Usage: query [arguments]"

  parser.on("-u USERNAME", "--user=USERNAME", "Username to send to Trino ('#{user}')") { |u| user = u }
  parser.on("-w PASSWORD", "--password=PASSWORD", "Password to send to Trino ('#{password}')") { |p| password = p }
  parser.on("-h HOSTNAME", "--host=HOSTNAME", "Hostname/IP to connect to Trino ('#{hostname}')") { |h| hostname = h }
  parser.on("-p PORT", "--port=PORT", "Port to connect to Trino (#{port})") { |p| post = p }
  parser.on("--help", "Show this help") { puts parser; exit }
end

use_ssl = !password.nil?

url_str = if password
            "#{user}:#{password}@#{hostname}:#{port}"
          else
            "#{hostname}:#{port}"
          end

client = TrinoClient.new(url_str, user, use_ssl)

resp = nil
elapsed = Time.measure { resp = client.query(STDIN.gets_to_end) }
puts resp.to_pretty_json
STDERR.puts "elapsed: #{elapsed}"
