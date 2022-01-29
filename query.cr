require "http/client"
require "uuid"
require "json"

class PrestoClient
  def initialize(@host_port : String, @user : String, @catalog : String, @schema : String); end

  def query(query : String, options : Hash(Symbol, String) = {} of Symbol => String)
    response = initial_request(query)
    body = JSON.parse(response.body)

    next_uri = body["nextUri"]
    state = body["stats"].not_nil!["state"]

    data = [] of Hash(String, String)
    loop do
      body = follow_up(next_uri, state, options)
      data = accumulate(data, body)
      break if complete?(body)
      next_uri = body["nextUri"]
    end

    if errored?(body)
      {status: "FAILED", error: body["error"]}
    else
      {status: "OK", data: data.flatten}
    end
  end

  private def complete?(body)
    %w{FINISHED FAILED}.includes?(body.dig("stats", "state"))
  end

  private def errored?(body)
    body.dig("stats", "state") == "FAILED"
  end

  private def accumulate(acc, body)
    b = body["data"].as_a
    acc + b
  rescue KeyError
    acc
  end

  private def initial_request(query)
    HTTP::Client.post(
      "http://#{@host_port}/v1/statement",
      HTTP::Headers{
        "X-Presto-User"    => @user,
        "X-Trino-User"     => @user,
        "X-Presto-Source"  => "Crystal client",
        "X-Trino-Source"   => "Crystal client",
        "X-Presto-Catalog" => @catalog,
        "X-Trino-Catalog"  => @catalog,
        "X-Presto-Schema"  => @schema,
        "X-Trino-Schema"   => @schema,
      },
      query
    )
  end

  private def follow_up(url, state, options)
    if %w{QUEUED RUNNING}.includes?(state)
      response = HTTP::Client.get(url.to_s)
      JSON.parse(response.body)
    else
      JSON::Any.new(nil)
    end
  end
end

client = PrestoClient.new("127.0.0.1:8080", "kmatthias", "cassandra", "summaries_test")
resp = client.query(<<-EOF
  SELECT bv.client_id, bv.tag_key, bv.value
  FROM cassandra.subscription_data.by_value AS bv
  WHERE bv.client_id = '71d32975-d8df-4906-a1bf-4fa493528532'
    AND bv.tag_key = 'club'
    AND bv.value in ('lions', 'elks')
  LIMIT 50
EOF
)

pp resp
