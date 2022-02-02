require "./spec_helper"
require "webmock"

FIXTURES_PATH = if __FILE__ == ""
                  File.expand_path(
                    File.join(File.dirname(__FILE__), "fixtures")
                  )
                else
                  File.expand_path(
                    File.join(File.dirname(__FILE__), "..", "fixtures")
                  )
                end

Spectator.describe TrinoClient::Client do
  let :client { TrinoClient::Client.new("localhost:8080", "beowulf", use_ssl: false) }

  after_all do
    WebMock.reset
  end

  it "instantiates a client" do
    expect(client).not_to be_nil
  end

  describe "when successful" do
    before_each do
      mock_series("select_1")
    end

    after_each do
      WebMock.reset
    end

    it "handles a successful query" do
      result = client.query("SELECT 1 AS heorot")

      expect(result.has_error?).to be_false
      expect(result.data.size).to eq(1)
      expect(result.data.first).to eq({"heorot" => 1})
    end
  end

  describe "supporting various data types" do
    before_each do
      mock_series("select_data_types")
    end

    after_each do
      WebMock.reset
    end

    it "handles a real result set with different data types" do
      result = client.query("
        SELECT 'Some text' AS t, 1.0000005 AS d, count(1) AS count,
          timestamp '2022-02-01 18:43 UTC' AS time,
          UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59' AS uuid
      ")

      expect(result.data.size).to eq(1)

      row = result.data.first
      expect(row["t"]).to eq("Some text")
      expect(row["d"]).to eq(1.0000005)
      expect(row["count"]).to eq(9001)
      expect(row["time"]).to eq(Time.utc(2022, 2, 1, 18, 43, 0))
      expect(row["uuid"]).to eq(UUID.new("12151fd2-7586-11e9-8f9e-2a86e4085a59"))
    end
  end

  describe "when failing" do
    after_each do
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
      WebMock.stub(:post, "http://localhost:8080/v1/statement")
        .with(body: "SELECT FOO()")
        .to_return(status: 200, body: get_fixture("function_not_found.json"))

      result = client.query("SELECT FOO()")

      expect(result.status).to eq("FAILED")
      expect(result.data).to be_empty
      expect(result.has_error?).to be_true
      expect(result.error).not_to be_nil
      expect(result.error.not_nil!["message"]).to match(/Function.*not registered/)
    end
  end
end

def get_fixture(name)
  File.read(File.join(FIXTURES_PATH, name))
end

def mock_series(base_dir)
  files = Dir[File.join([FIXTURES_PATH, base_dir, "*.json"])].sort

  WebMock.stub(:post, "http://localhost:8080/v1/statement").to_return(
    status: 200, body: File.read(files.shift)
  )

  files.each_with_index do |fname, i|
    WebMock.stub(:get, "http://localhost:8080/number_#{i + 1}").to_return(
      status: 200, body: File.read(fname)
    )
  end
end
