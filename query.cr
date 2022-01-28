require "http/client"
require "uuid"
require "json"

class PrestoResponse
  include Enumerable(PrestoResponse)

  def initialize(@response : JSON::Any); end

end

class PrestoClient
  def initialize(@host_port : String, @user : String, @catalog : String, @schema : String); end

  def query(query : String, options : Hash(Symbol, String) = {} of Symbol => String)
    response = initial_request(query)
    p response.body

    resp = JSON.parse(response.body)
    next_uri = resp["nextUri"]
    state = resp["stats"].not_nil!["state"]

    data = [] of JSON::Any | String
    loop do
      puts next_uri
      resp = follow_up(next_uri, state, options)
      accumulate(data, resp) == :end

      break if %w{ FINISHED FAILED }.includes?(resp.dig("stats", "state"))

      next_uri = resp["nextUri"]
    end

    p data
  end

  private def accumulate(acc, resp)
      acc << resp["data"]
    rescue KeyError
  end

  private def initial_request(query)
    HTTP::Client.post(
      "http://#{@host_port}/v1/statement",
      HTTP::Headers{
        "X-Presto-User" => @user,
        "X-Trino-User" => @user,
        "X-Presto-Source" => "Crystal client",
        "X-Trino-Source" => "Crystal client",
        "X-Presto-Catalog" => @catalog,
        "X-Trino-Catalog" => @catalog,
        "X-Presto-Schema" => @schema,
        "X-Trino-Schema" => @schema,
      },
      query
    )
  end

  private def follow_up(url, state, options)
    if %w{ QUEUED RUNNING }.includes?(state)
      response = HTTP::Client.get(url.to_s)
      p response.body
      JSON.parse(response.body)
    else
      {} of String => String
    end
  end
end

client = PrestoClient.new("127.0.0.1:8080", "kmatthias", "cassandra", "summaries_test")
resp = client.query(<<-EOF
  SELECT bv.client_id, bv.tag_key, bv.value, bv2.tag_key, bv2.value, bv.subscription_id, subs.first_name, subs.last_name
  FROM cassandra.subscription_data.by_value AS bv
    INNER JOIN postgresql.public.subscriptions AS subs
      ON subs.subscription_id = cast(bv.subscription_id AS UUID)
    INNER JOIN cassandra.subscription_data.by_value AS bv2
      ON bv.subscription_id = bv2.subscription_id
  WHERE bv.client_id = '71d32975-d8df-4906-a1bf-4fa493528532'
    AND bv2.client_id = bv.client_id
    AND bv.tag_key = 'club'
    AND bv.value in ('lions', 'elks')
    AND bv2.tag_key = 'donation'
    AND bv2.value in ('000001000', '000002000')
EOF
)
