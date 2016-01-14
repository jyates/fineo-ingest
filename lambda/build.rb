#!/usr/bin/env ruby

path = File.dirname(__FILE__)
confDir = ENV["CONFIG_DIR"] || "#{path}/config"
confFile = ENV["CONFIG_FILE"] || "#{confDir}/fineo-lambda.properties"
debug = ENV["DEBUG"] || "t"

require 'optparse'

# Value, file key suffix
#
# Pair values are the suffix and map to the names that the FirehoseClientProperties uses to parse
# information from the properties file. They will all be prefixed with 'fineo.' on write
Pair = Struct.new(:one, :two)

# Populate the options with default values and the correct key suffix
opts = {
  :kinesis => Pair.new("kinesis.us-east-1.amazonaws.com", "kinesis.url"),
  :parsed => Pair.new("fineo-parsed-records", "kinesis.parsed"),

  :firehose => Pair.new("https://firehose.us-east-1.amazonaws.com", "firehose.url"),
  :malformed => Pair.new("fineo-malformed-records", "firehose.malformed"),
  :staged => Pair.new("fineo-staged-recods", "firehose.staged"),
  :staged_dynamo_error => Pair.new("fineo-staged-error-recods", "firehose.staged.error.dynamo"),

  :dynamo => Pair.new("https://dynamodb.us-east-1.amazonaws.com", "dynamo.url"),
  :schema_table => Pair.new("customer-schema", "dynamo.schema-store"),
  :ingest_prefix => Pair.new("customer-ingest", "dynamo.ingest.prefix")
  :write_max => Pair.new("100", "dynamo.limit.write")
  :read_max => Pair.new("1000", "dynamo.limit.read")
  :retries => Pair.new("3", "dynamo.limit.retries")
}

# set pair value at option[ref]
def set(options, ref, value)
  options[ref].one = value
end

file = File.basename(__FILE__)
parser = OptionParser.new do|opts|
  opts.banner = "Usage: #{file} [options]"
  opts.on('-k', '--kinesis-url kinesis-url', 'Kinesis address - not a URL') do |url|
    set(options, :kinesis, url)
  end
  opts.on('-p', '--parsed-stream stream name', 'Parsed Avro record Kinesis stream name') do |name|
    set options, :parsed, name
  end

  opts.on('-f', '--firehose-url firehose-url', 'Firehose Url') do |url|
    set options, :firehose, url
  end
  opts.on('-m', '--malformed-stream stream-name', 'Malformed event Kinesis Firehose stream name')
   do |name|
    set options, :malformed, name
  end
  opts.on('-t, '--staged-stream stream-name', 'Staged avro event Kinesis Firehose stream name')  do |name|
    set options, :staged, name
  end
  opts.on('-e, '--staged-dynamo-error-stream stream-name', 'Kinesis Firehose stream' +
    'name for messages that could not be written dynamo')  do |name|
    set options, :staged_dynamo_error, name
  end

  opts.on('d', '--dynamo-url dynamo-url', 'DynamoDB Endpoint Url') do |url|
    set options, :dynamo, url
  end
  opts.on('s', '--dynamo-schema-table table-name', 'DynamoDB schema repository table name') do |name|
    set options, :schema_table, name
  end
  opts.on('i', '--dynamo-ingest-prefix table-prefix', 'DynamoDB ingest table name prefix') do |name|
    set options, :ingest_prefix, name
  end
  opts.on('r', '--dynamo-read-limit limit', 'Max amount of read units to allocate to a single table')
   do |name|
    set options, :read_max, name
  end
  opts.on('w', '--dynamo-write-limit limit', 'Max amount of write units to allocate to a single table')
    do |name|
    set options, :write_max, name
  end
    opts.on('l', '--dynamo-max-retries limit', 'Max amount of write units to allocate to a single table')
      do |name|
      set options, :retries, name
    end

  opts.on('-h', '--help', 'Displays Help') do
    puts opts
    exit
  end
end

parser.parse!

# create the directories if they don't exist
Dir.mkdir(confDir) unless Dir.exists? confDir

def add_opt(file, pair)
  file.puts "fineo.#{pair.two}=#{pair.one}"
end


File.open(confFile, 'w') do |file|
  opts.values.each { |pair|
    add_opt file, pair
  }
end

# Read back in the file to the console so we know what got written
unless debug.nil?
  File.open(confFile, "r") do |f|
    f.each_line do |line|
      puts line
    end
  end
end

# Build the package
system "mvn clean package -Ddeploy"

jar = Dir["#{path}/target/lambda-*.jar"]
puts "Upload #{jar[0]} to AWS"