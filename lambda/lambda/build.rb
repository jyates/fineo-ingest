#!/usr/bin/env ruby

path = File.dirname(__FILE__)
confDir = ENV["CONFIG_DIR"] || "#{path}/config"
confFile = ENV["CONFIG_FILE"] || "#{confDir}/fineo-lambda.properties"

# Value to write into the client properties .They will all be prefixed with 'fineo.' on write.
#
# [default value, conf key]
Pair = Struct.new(:one, :two)

# Defaults overridable by options
require 'ostruct'
$config = OpenStruct.new
$config.skip = ""
$config.test = nil
$config.verbose = false

# Populate the options with default values and the correct key suffix
$options = {
  :kinesis => Pair.new("kinesis.us-east-1.amazonaws.com", "kinesis.url"),
  :parsed => Pair.new("fineo-parsed-records", "kinesis.parsed"),

  :firehose => Pair.new("https://firehose.us-east-1.amazonaws.com", "firehose.url"),
  :malformed => Pair.new("fineo-raw-error-records", "firehose.raw.error"),
  :staged => Pair.new("fineo-staged-records", "firehose.staged"),
  :staged_error => Pair.new("fineo-staged-error-recods", "firehose.staged.error"),

  :dynamo => Pair.new("us-east-1", "dynamo.region"),
  :schema_table => Pair.new("schema-customer", "dynamo.schema-store"),
  :ingest_prefix => Pair.new("customer-ingest", "dynamo.ingest.prefix"),
  :write_max => Pair.new("5", "dynamo.limit.write"),
  :read_max => Pair.new("7", "dynamo.limit.read"),
  :retries => Pair.new("3", "dynamo.limit.retries")
}

# set pair value at option[ref]
def set(ref, value)
  $options[ref].one = value
end

require 'optparse'
file = File.basename(__FILE__)
parser = OptionParser.new do|opts|
  opts.banner = "Usage: #{file} [options]"

  opts.separator "Kinesis Options:"
  opts.on('-k', '--kinesis-url kinesis-url', 'Kinesis address - not a URL') do |url|
    set :kinesis, url
  end
  opts.on('-p', '--parsed-stream stream name', 'Parsed Avro record Kinesis stream name') do |name|
    set :parsed, name
  end

  opts.separator "Firehose Options:"
  opts.on('-f', '--firehose-url firehose-url', 'Firehose Url') do |url|
    set :firehose, url
  end
  opts.on('-m', '--malformed-stream stream-name', 'Malformed event Kinesis Firehose stream name') do |name|
    set :malformed, name
  end
  opts.on('-t', '--staged-stream stream-name', 'Staged avro event Kinesis Firehose stream name') do |name|
    set :staged, name
  end
  opts.on('-e', '--staged-dynamo-error-stream stream-name', 'Kinesis Firehose stream' +
    'name for messages that could not be written dynamo')  do |name|
      set :staged_error, name
  end

  opts.separator "Dynamo Options:"
  opts.on('-d', '--dynamo-url dynamo-url', 'DynamoDB Endpoint Url') do |url|
    set :dynamo, url
  end
  opts.on('-s', '--dynamo-schema-table table-name', 'DynamoDB schema repository table name') do |name|
    set :schema_table, name
  end
  opts.on('-i', '--dynamo-ingest-prefix table-prefix', 'DynamoDB ingest table name prefix') do |name|
    set :ingest_prefix, name
  end
  opts.on('-r', '--dynamo-read-limit limit', 'Max amount of read units to allocate to a '+
    'single table') do |name|
      set :read_max, name
    end
  opts.on('-w', '--dynamo-write-limit limit', 'Max amount of write units to allocate to a ' +
    'single table')do |name|
      set :write_max, name
    end
  opts.on('-l', '--dynamo-max-retries limit', 'Max amount of retries to attempt before failing '+
    'the request') do |name|
      set :retries, name
  end

  opts.separator ""
  opts.on("--test aws-key", "Build 'test' parameters pointing the resources keyed with the " +
  "aws-key. This is different than the aws_acceess_key, but instead just a suffix attached to" +
  "resources to identify it. Also some resources are prefixed with 'test-'") do |key|
    $config.test = key
    $config.verbose = true
  end
  opts.on("--skip-tests", "Skip running tests when building deployable jar") do |s|
    $config.skip ="-DskipTests"
  end
  opts.on("-v", "--verbose", "Verbose output") do |v|
    $config.verbose = true
  end
  opts.on("-vv", "--extra-verbose", "Extra Verbose output") do |v|
    $config.verbose = true
    $config.verbose2 = true
  end
  opts.on('-h', '--help', 'Displays Help') do
    puts opts
    exit
  end
end

parser.parse!

def test_fix(name)
  "test-"+$options[name].one+"-#{$config.test}"
end

unless $config.test.nil?
  set :parsed, test_fix(:parsed)
  set :malformed, test_fix(:malformed)
  set :staged, test_fix(:staged)
  set :staged_error, test_fix(:staged_error)
  set :schema_table, test_fix(:schema_table)
  set :ingest_prefix, test_fix(:ingest_prefix)
end

# create the directories if they don't exist
Dir.mkdir(confDir) unless Dir.exists? confDir
File.open(confFile, 'w') do |file|
  $options.values.each { |pair|
   file.puts "fineo.#{pair.two}=#{pair.one}"
  }
end

# Read back in the file to the console so we know what got written
if $config.verbose
  File.open(confFile, "r") do |f|
    f.each_line do |line|
      puts line
    end
  end
end

# Build the package
cmd="mvn -f #{path} clean install -Ddeploy #{$config.skip_tests}"
if $config.verbose2
  puts "Running: #{cmd}"
  system cmd
end
`#{cmd}`
puts "---> [Done]"