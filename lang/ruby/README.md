Avro for Ruby
=============

[Apache Avro] is a data serialization system that is a boon to integrating Ruby
in heterogeneous data processing systems and distributed services. Avro
supports defining RPC protocols, provides a file format for persistent data
storage designed to facilitate efficient map reduce, and is friendly to dynamic
languages (no code generation required).

Usage
-----

Following are short examples of key features. See below for complete API
documentation.

### Data File Writing and Reading ###

```ruby
require 'avro'

# You can of course read schemata from files or elsewhere, we show an inline
# string here for illustrative purposes.
USER_SCHEMA = <<-JSON
{ "type": "record",
  "name": "User",
  "fields" : [
    { "name": "username", "type": "string" },
    { "name": "age", "type": "int" },
    { "name": "verified", "type": "boolean", "default": "false" }
  ]}
JSON

# Write records conforming to the schema
Avro::DataFile.open('users.avr', 'w', USER_SCHEMA) do |writer|
  writer << { 'username' => 'sally', 'age' => 29, 'verified' => true }
  writer << { 'username' => 'bob', 'age' => 34, 'verified' => false }
end

# In read mode, open returns or yields an Enumerable object giving access to the
# records in the file.
reader = Avro::DataFile.open('users.avr')
sally = reader.first
reader.close

sally['username']  # => 'sally'
sally['age']       # => 29
```

TODO: show a few enticing examples of major features.


Installation
------------

The library is published on RubyGems, so you can install by the customary means:

    $ gem install avro

Or in a Bundler project, add to your `Gemfile`:

    gem 'avro'

and run `bundle install`.

API Documentation
-----------------

Find the latest as well as previous releases on RubyDoc.info:

<http://www.rubydoc.info/gems/avro>

To build the API documentation locally, run `./build.sh doc` in the project
directory.

Contributing
------------

See the [How to Contribute] page on the Avro project wiki for instructions on
building the project and running tests. Please note that *pull requests are not
accepted through GitHub*. Contributions are submitted as patches, so you are
welcome to use Git and the GitHub repository mirror, as long as you are
comfortable with the tools for generating patches and complying with the
contribution guidelines.


[Apache Avro]: http://avro.apache.org/
[How to Contribute]: https://cwiki.apache.org/confluence/display/AVRO/How+To+Contribute

