require "http/client"
require "uuid"
require "json"

alias AllValues = String | Int64 | Float64 | Time | UUID
alias DataValue = Hash(String, AllValues)

class TrinoClient::QueryError < RuntimeError; end

# A Response struct represents the results of a Trino query.
struct TrinoClient::Response
  include JSON::Serializable
  getter :status, :data, :error

  def initialize(@status : String, @data : Array(DataValue), @error : DataValue? = nil); end

  def has_error?
    !@error.nil?
  end
end

class TrinoClient::Client
  def initialize(@host_port : String, user : String, @use_ssl : Bool)
    @pool = StringPool.new
    @headers = HTTP::Headers{
      "X-Presto-User"   => user,
      "X-Trino-User"    => user,
      "X-Presto-Source" => "Crystal client",
      "X-Trino-Source"  => "Crystal client",
    }
  end

  def query(query : String, options : Hash(Symbol, String) = {} of Symbol => String) : TrinoClient::Response
    raise QueryError.new("Invalid query string") if query.size < 7

    # Strip off any trailing semi-colon so we don't error
    query = query.chomp.rstrip(";")

    body = initial_request(query)

    state = body["stats"].not_nil!["state"]

    data = [] of JSON::Any
    columns = {} of String => String
    if %w{QUEUED RUNNING}.includes?(state)
      loop do
        next_uri = body["nextUri"]
        body = advance(next_uri, state, options)
        data = accumulate(data, body)
        if complete?(body)
          columns = get_columns(body)
          break
        end
      end
    end

    # Check again for the follow up result
    if errored?(body)
      return TrinoClient::Response.new(status: "FAILED", data: [] of DataValue, error: extract_error(body))
    end

    rows = data.map do |row|
      row = row.as_a
      values = columns.values.each_with_index.map { |v, i| get_type(row[i], v) }
      columns.keys.zip(values).to_h
    end.as(Array(DataValue))

    TrinoClient::Response.new(status: "OK", data: rows)
  end

  private def initial_request(query) : JSON::Any
    response = with_retries { HTTP::Client.post("#{protocol}://#{@host_port}/v1/statement", @headers, query) }
    JSON.parse(response.body)
  rescue e : JSON::ParseException
    raise TrinoClient::QueryError.new("Bad query response: #{e.message}")
  end

  private def protocol
    if @use_ssl
      "https"
    else
      "http"
    end
  end

  private def with_retries(&block)
    retries = [60, 70, 80, 90, 100].map(&.milliseconds)

    while (sleep_time = retries.shift)
      response = yield
      # Trino docs say anything other than 503 and 200 is a failure
      case response.status_code
      when 503 then sleep(sleep_time); next
      when 200 then break
      else
        raise TrinoClient::QueryError.new("Failed " + response.not_nil!.body.inspect)
      end
    end

    response.not_nil!
  end

  # Trino returns a pretty huge error structure. This just grabs out the parts
  # that are useful on the client side and string pools the values so we don't
  # bloat up memory on repeated failures.
  private def extract_error(body) : DataValue
    err = body["error"]
    {
      "message"  => @pool.get(err["message"].to_s).as(AllValues),
      "name"     => @pool.get(err["errorName"].to_s).as(AllValues),
      "type"     => @pool.get(err["errorType"].to_s).as(AllValues),
      "location" => "line #{err.dig("errorLocation", "lineNumber")}, column #{err.dig("errorLocation", "columnNumber")}".as(AllValues),
    }.as(DataValue)
  end

  # Do type conversion based on the type names returned from the Trino API
  # response.
  private def get_type(key, type)
    case type
    when "integer"  then key.as_i64
    when "bigint"   then key.as_i64
    when "string"   then key.as_s
    when "text"     then key.as_s
    when "uuid"     then UUID.new(key.as_s)
    when "float"    then key.as_f
    when /^varchar/ then key.as_s
    when /^decimal/ then key.as_s.to_f
    when /time/     then parse_time(key.as_s)
    else
      raise QueryError.new("Unknown type returned: #{type}")
    end
  end

  # Attempt to parse out a timestamp that is an ISO8601 UTC timestamp if it
  # passes the basic regex
  private def parse_time(time_str : String)
    Time.parse(time_str, "%F %H:%M:%S.%6N", Time::Location::UTC)
  rescue Time::Format::Error
    time_str
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
    body["columns"].as_a.map do |c|
      {@pool.get(c["name"].as_s), @pool.get(c["type"].as_s)}
    end.to_h
  rescue KeyError
    {} of String => String
  end

  # Make a GET request to the next URI in the chain and parse the response
  # body.
  private def advance(url, state, options) : JSON::Any
    response = with_retries { HTTP::Client.get(url.to_s) }
    JSON.parse(response.body)
  end

  private def complete?(body)
    %w{FINISHED FAILED}.includes?(body.dig("stats", "state"))
  end

  private def errored?(body)
    body.dig("stats", "state") == "FAILED"
  end
end
