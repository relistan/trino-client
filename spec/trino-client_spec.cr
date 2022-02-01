require "./spec_helper"
require "webmock"

Spectator.describe TrinoClient::Client do
  let :client { TrinoClient::Client.new("localhost:8080", "beowulf", use_ssl: false) }

  describe "when successful" do
    #before_all do
    #  WebMock.stub(:post, "http://localhost:8080/v1/statement").to_return(
    #    status: 200, body: ""
    #  )
    #end

    it "instantiates a client" do
      expect(client).not_to be_nil
    end

    it "handles a real result set with different data types" do
      result = client.query("
        SELECT 'Some text' AS t, 1.0000005 AS d, count(1) AS count,
          timestamp '2022-02-01 18:43 UTC' AS time,
          UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59' AS uuid
        FROM postgresql.public.subscriptions
      ")

      expect(result.data.size).to eq(1)

      row = result.data.first
      expect(row["t"]).to eq("Some text")
      expect(row["d"]).to eq(1.0000005)
      expect(row["count"]).to eq(9001)
      expect(row["time"]).to eq(Time.utc(2022, 2, 1, 18, 43, 0))
      expect(row["uuid"]).to eq(UUID.new("12151fd2-7586-11e9-8f9e-2a86e4085a59"))
    end

    it "handles a successful query" do
      result = client.query("SELECT 1 AS heorot")

      expect(result.has_error?).to be_false
      expect(result.data.size).to eq(1)
      expect(result.data.first).to eq({"heorot" => 1})
    end
  end

  describe "when failing" do
    before_each do
      WebMock.reset
    end

    it "handles a truncated response" do
      WebMock.stub(:post, "http://localhost:8080/v1/statement").to_return(
        status: 200, body: ""
      )

      expect {
        client.query("SELECT 1 AS heorot")
      }.to raise_error(TrinoClient::QueryError, message: /Bad query response/)
    end

    it "handles a failed query: too short" do
      expect {
        result = client.query("SEL")
      }.to raise_error(TrinoClient::QueryError, message: /Invalid query string/)
    end

    it "handles a failed query: bad syntax" do
      result = client.query("SELECT FOO()")

      expect(result.status).to eq("FAILED")
      expect(result.data).to be_empty
      expect(result.has_error?).to be_true
      expect(result.error.not_nil!.keys.sort).to eq(%w{location message name type})
    end
  end
end
