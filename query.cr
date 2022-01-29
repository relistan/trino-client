require "http/client"
require "uuid"
require "json"

class TrinoQueryError < RuntimeError; end

class TrinoClient
  def initialize(@host_port : String, @user : String, @password : String? = nil)
    @pool = StringPool.new
  end

  def query(query : String, options : Hash(Symbol, String) = {} of Symbol => String)
    raise TrinoQueryError.new("Invalid query string") if query.size < 7


    response = initial_request(query)
    body = JSON.parse(response.body)

    next_uri = body["nextUri"]
    state = body["stats"].not_nil!["state"]

    data = [] of JSON::Any
    columns = [] of String
    loop do
      body = advance(next_uri, state, options)
      data = accumulate(data, body)
      if complete?(body)
        columns = get_columns(body)
        break
      end
      next_uri = body["nextUri"]
    end

    if errored?(body)
      {status: "FAILED", error: body["error"]}
    else
      rows = data.map { |row| columns.zip(row.as_a).to_h }
      {status: "OK", data: rows}
    end
  end

  private def initial_request(query)
    retries = [60, 70, 80, 90, 100].map(&.milliseconds)
    response = HTTP::Client::Response.new(400, "No requests were made?")

    while (sleep_time = retries.pop)
      response = HTTP::Client.post(
        "http://#{@host_port}/v1/statement",
        HTTP::Headers{
          "X-Presto-User"   => @user,
          "X-Trino-User"    => @user,
          "X-Presto-Source" => "Crystal client",
          "X-Trino-Source"  => "Crystal client",
        },
        query
      )

      # Trino docs say anything other than 503 and 200 is a failure
      case response.status_code
      when 503 then sleep(sleep_time); next
      when 200 then break
      else
        raise TrinoQueryError.new("Failed " + response.not_nil!.body.inspect)
      end
    end

    response
  end

  private def accumulate(acc, body)
    b = body["data"].as_a
    acc + b
  rescue KeyError
    acc
  end

  private def get_columns(body)
    body["columns"].as_a.map { |c| @pool.get(c["name"].as_s) }
  rescue KeyError
    [] of String
  end

  private def advance(url, state, options)
    if %w{QUEUED RUNNING}.includes?(state)
      response = HTTP::Client.get(url.to_s)
      JSON.parse(response.body)
    else
      JSON::Any.new(nil)
    end
  end

  private def complete?(body)
    %w{FINISHED FAILED}.includes?(body.dig("stats", "state"))
  end

  private def errored?(body)
    body.dig("stats", "state") == "FAILED"
  end
end

client = TrinoClient.new("127.0.0.1:8080", "kmatthias")
start_time = Time.local
resp = client.query(ARGF.gets_to_end)
elapsed = "elapsed: #{Time.local - start_time}"

puts resp.to_pretty_json
STDERR.puts elapsed
