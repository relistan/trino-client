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

    body = initial_request(query)
    state = body["stats"].not_nil!["state"]

    data = [] of JSON::Any
    columns = [] of String
    loop do
      next_uri = body["nextUri"]
      body = advance(next_uri, state, options)
      data = accumulate(data, body)
      if complete?(body)
        columns = get_columns(body)
        break
      end
    end

    if errored?(body)
      {status: "FAILED", error: extract_error(body)}
    else
      rows = data.map { |row| columns.zip(row.as_a).to_h }
      {status: "OK", data: rows}
    end
  end

  private def initial_request(query) : JSON::Any
    response = with_retries do
      HTTP::Client.post(
        "http://#{@host_port}/v1/statement",
        HTTP::Headers{
          "X-Presto-User"   => @user,
          "X-Trino-User"    => @user,
          "X-Presto-Source" => "Crystal client",
          "X-Trino-Source"  => "Crystal client",
        },
        query
      )
    end

    JSON.parse(response.body)
  end

  private def with_retries(&block)
    retries = [60, 70, 80, 90, 100].map(&.milliseconds)

    while (sleep_time = retries.pop)
      response = yield
      # Trino docs say anything other than 503 and 200 is a failure
      case response.status_code
      when 503 then sleep(sleep_time); next
      when 200 then break
      else
        raise TrinoQueryError.new("Failed " + response.not_nil!.body.inspect)
      end
    end

    response.not_nil!
  end

  # Trino returns a pretty huge error structure. This just grabs out the parts
  # that are useful on the client side and string pools the values so we don't
  # bloat up memory on repeated failures.
  private def extract_error(body)
    err = body["error"]
    {
      message:  @pool.get(err["message"].to_s),
      name:     @pool.get(err["errorName"].to_s),
      type:     @pool.get(err["errorType"].to_s),
      location: "line #{err.dig("errorLocation", "lineNumber")}, column #{err.dig("errorLocation", "columnNumber")}",
    }
  end

  # Wrap the fetch from the hash and if the key was missing, just return the
  # value that was passed in as the accumulator.
  private def accumulate(acc, body)
    b = body["data"].as_a
    acc + b
  rescue KeyError
    acc
  end

  # Grab the columns definition from the response, extract the names, and stick
  # them into an array as Strings.
  private def get_columns(body)
    body["columns"].as_a.map { |c| @pool.get(c["name"].as_s) }
  rescue KeyError
    [] of String
  end

  # Make a GET request to the next URI in the chain and parse the response
  # body.
  private def advance(url, state, options) : JSON::Any
    if %w{QUEUED RUNNING}.includes?(state)
      response = with_retries { HTTP::Client.get(url.to_s) }
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
