#!/usr/bin/env ruby

path = File.expand_path(File.dirname(__FILE__))+"/../"
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
$config.verbose2 = false

# Populate the options with default values and the correct key suffix
$options = {
  :kinesis => Pair.new("kinesis.us-east-1.amazonaws.com", "kinesis.url"),
  :parsed => Pair.new("fineo-parsed-records", "kinesis.parsed"),
  :kinesis_retries => Pair.new("3", "kinesis.retries"),

  :firehose => Pair.new("https://firehose.us-east-1.amazonaws.com", "firehose.url"),
  # raw processing params
  :raw_archive => Pair.new("fineo-raw-archive", "firehose.raw.archive"),
  :raw_malformed => Pair.new("fineo-raw-malformed", "firehose.raw.error"),
  :raw_error => Pair.new("fineo-raw-commit-failure", "firehose.raw.error.commit"),
  # parsed record - "staged" - params
  :staged_archive => Pair.new("fineo-staged-archive", "firehose.staged.archive"),
  :staged_error_dynamo => Pair.new("fineo-staged-dynamo-error", "firehose.staged.error"),
  :staged_error => Pair.new("fineo-staged-commit-failure", "firehose.staged.error.commit"),

  :dynamo => Pair.new("us-east-1", "dynamo.region"),
  :schema_table => Pair.new("schema-customer", "dynamo.schema-store"),
  :ingest_prefix => Pair.new("customer-ingest", "dynamo.ingest.prefix"),
  :write_max => Pair.new("5", "dynamo.limit.write"),
  :read_max => Pair.new("7", "dynamo.limit.read"),
  :dynamo_retries => Pair.new("3", "dynamo.limit.retries")
}

test_properties = [:schema_table, :ingest_prefix, :parsed, :raw_archive, :staged_archive]
# Firehose name used for all 'error' firehoses when we are building a test deployment. This helps
# us get under the firehose limit but doesn't loose any clarity.
test_firehose_name="failed-records"
test_firehoses_rename = [:raw_error, :raw_malformed, :staged_error_dynamo, :staged_error]

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
  opts.on('--parsed-stream stream name', 'Parsed Avro record Kinesis stream name') do |name|
    set :parsed, name
  end
  opts.on('--kinesis-max-retries limit', 'Max amount of retries to attempt before failing '+
    'the request') do |name|
      set :kinesis_retries, name
  end

  opts.separator "Firehose Options:"
  opts.on('--firehose-url firehose-url', 'Firehose Url') do |url|
    set :firehose, url
  end
  opts.on('--raw-malformed-stream stream-name', 'Malformed event Kinesis Firehose stream name') do |name|
    set :raw_malformed, name
  end
  opts.on('--raw-failed-stream stream-name', 'Malformed event Kinesis Firehose stream name') do |name|
    set :raw_error, name
  end
  opts.on('--staged-stream stream-name', 'Name of Firehose stream to archive all staged records') do |name|
    set :staged_archive, name
  end
  opts.on('--staged-error-stream stream-name', 'Kinesis Firehose stream' +
    'name for messages that could not be handled properly')  do |name|
      set :staged_error, name
  end
  opts.on('--staged-dynamo-error-stream stream-name', 'Kinesis Firehose stream' +
    'name for messages that could not be written dynamo')  do |name|
      set :staged_error_dynamo, name
  end

  opts.separator "Dynamo Options:"
  opts.on('-d', '--dynamo-url dynamo-url', 'DynamoDB Endpoint Url') do |url|
    set :dynamo, url
  end
  opts.on('--dynamo-schema-table table-name', 'DynamoDB schema repository table name') do |name|
    set :schema_table, name
  end
  opts.on('--dynamo-ingest-prefix table-prefix', 'DynamoDB ingest table name prefix') do |name|
    set :ingest_prefix, name
  end
  opts.on('--dynamo-read-limit limit', 'Max amount of read units to allocate to a '+
    'single table') do |name|
      set :read_max, name
    end
  opts.on('--dynamo-write-limit limit', 'Max amount of write units to allocate to a ' +
    'single table')do |name|
      set :write_max, name
    end
  opts.on('--dynamo-max-retries limit', 'Max amount of retries to attempt before failing '+
    'the request') do |name|
      set :dynamo_retries, name
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
  opts.on("--vv", "Extra Verbose output") do |v|
    $config.verbose = true
    $config.verbose2 = true
  end
  opts.on('-h', '--help', 'Displays Help') do
    puts opts
    exit
  end
end

parser.parse!

def test_fix(prefix, name)
  val = $options[name].one
  "#{prefix}#{val}"
end

def log
  if $config.verbose
    yield
  end
  return $config.verbose
end

def vlog
  if $config.verbose2
    yield
  end
  return $config.verbose2
end

unless $config.test.nil?
  test_prefix = "test-#{$config.test}-"

  # store the test prefix so we can use it for testing
  test_key = :"integration.test.prefix"
  vlog { puts "Setting #{test} = #{test_prefix}" }
  $options[test_key] = Pair.new(test_prefix, test_key)

  # update the parameters
  test_properties.each{|property|
    set property, test_fix(test_prefix, property)
  }
  test_firehoses_rename.each{|property|
    set property, "#{test_prefix}#{test_firehose_name}"
  }
end

# create the directories if they don't exist
Dir.mkdir(confDir) unless Dir.exists? confDir
File.open(confFile, 'w') do |file|
  $options.values.each { |pair|
   file.puts "fineo.#{pair.two}=#{pair.one}"
  }
end

# Read back in the file to the console so we know what got written
log {
  puts "Contents of properties file: "
  File.open(confFile, "r") do |f|
    f.each_line do |line|
      puts line
    end
  end
  puts
}

# Build the package
cmd="mvn -f #{path} clean install -Ddeploy #{$config.skip_tests}"
if log { puts "Running: #{cmd}"}
  system cmd
else
  `#{cmd}`
end
puts ($? == 0 ? "---> [Done]" : "-----> FAILURE!!")
