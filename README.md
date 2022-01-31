# trino-client

A command line client for Trino or PrestoDB that has the following advantages:

 * It's a binary, so very fast startup time compared to the JVM tool
 * Output is always valid JSON, as a single object
 * Error output is also JSON, in the same object format
 * Always sends elapsed time to stderr so useful for tracking performance
   of queries end-to-end.

## Installation

Available as binaries for some platforms. Otherwise you may compile with Crystal.

## Usage

The query string is read fron stdin and output in JSON format is to stdout.
Stderr will receive total elapsed time information. All queries are followed to
completion.

```
Usage: query [arguments]
    -u USERNAME, --user=USERNAME     Username to send to Trino ('kmatthias')
    -w PASSWORD, --password=PASSWORD Password to send to Trino ('')
    -h HOSTNAME, --host=HOSTNAME     Hostname/IP to connect to Trino ('localhost')
    -p PORT, --port=PORT             Port to connect to Trino (8080)
    --help                           Show this help
```

## Development

You may build the tool with:

```
crystal build query.cr
```

## Contributing

1. Fork it (<https://github.com/your-github-user/trino-client/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [Karl Matthias](https://github.com/your-github-user) - creator and maintainer
