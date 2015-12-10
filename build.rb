#!/usr/bin/env ruby

path = File.dirname(__FILE__)
confDir = ENV["CONFIG_DIR"] || "#{path}/config"
confFile = ENV["CONFIG_FILE"] || "#{confDir}/fineo-lambda.properties"
debug = ENV["DEBUG"] || "t"

require 'optparse'

Pair = Struct.new(:one, :two)
opts = {
  :kinesis => Pair.new("kinesis.us-east-1.amazonaws.com", "kinesis.url"),
  :parsed => Pair.new("fineo-parsed-records", "kinesis.parsed"),

  :firehose => Pair.new("https://firehose.us-east-1.amazonaws.com", "firehose.url"),
  :malformed => Pair.new("fineo-malformed-records", "firehose.malformed")
}

file = File.basename(__FILE__)
parser = OptionParser.new do|opts|
  opts.banner = "Usage: #{file} [options]"
  opts.on('-k', '--kinesis-url kinesis-url', 'Kinesis address - not a URL') do |url|
    options[:kinesis].one = url;
  end
  opts.on('-p', '--parsed-stream stream name', 'Parsed Avro record Kinesis stream name') do |name|
    options[:parsed].one = name;
  end

  opts.on('-f', '--firehose-url firehose-url', 'Firehose Url') do |url|
    options[:firehose].one = url;
  end
  opts.on('-m', '--malformed-stream stream name', 'Malformed event Kinesis Firehose stream name') do |name|
    options[:malformed].one = name;
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
  file.puts "#{pair.two}=#{pair.one}"
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