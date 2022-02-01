require "./spec_helper"

Spectator.describe TrinoClient::Client do
  let :client { TrinoClient::Client.new("localhost:8080", "beowulf", use_ssl = false) }

  it "instantiates a client" do
    expect(client).not_to be_nil
  end

  it "handles a successful query" do
    result = client.query("SELECT 1 AS heorot")

    expect(result.has_error?).to be_false
    expect(result.data.size).to eq(1)
    expect(result.data.first).to eq({"heorot" => 1})
  end

end
