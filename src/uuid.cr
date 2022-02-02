# Monkey patch UUID to support proper JSON serialization. This is necessary if
# we want to both support returning native UUID types and also work with the
# CLI tool that directly generates JSON.
struct UUID
  include JSON::Serializable

  def to_json(json : JSON::Builder)
    json.scalar(self.to_s)
  end
end

