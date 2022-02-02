require "./spec_helper"

Spectator.describe UUID do
  it "serializes to JSON" do
    id = UUID.new("605183db-de9d-4357-9d61-7fc979680f20")

    expect(id.to_json).to eq "\"#{id.to_s}\""
  end
end
