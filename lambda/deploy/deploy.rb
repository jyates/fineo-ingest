#!/usr/bin/env ruby

require 'pp'

# Constants
$PROP_FILE = "fineo-lambda.properties"

# File properties
path = File.dirname(__FILE__)
file = File.basename(__FILE__)
debug = ENV["DEBUG"] || "t"


# List of the functions and properties about them
functions = [
  {
    function_name: "RawToAvro",
    description: "Convert raw JSON records to avro encoded records",
    handler: "io.fineo.lambda.avro.LambdaRawRecordToAvro::handler",
    role: "arn:aws:iam::766732214526:role/Lambda-Raw-To-Avro-Ingest-Role",
    timeout: 10,
    memory_size: 128
  },
  {
    function_name: "AvroToStorage",
    description: "Stores the avro-formated bytes into Dynamo and S3",
    handler: "io.fineo.lambda.storage.LambdaAvroToStorage::handler",
    role: "arn:aws:iam::766732214526:role/Lambda-Dynamo-Ingest-Role",
    timeout: 10, # seconds
    memory_size: 128 # MB
  }
]

def update(lambda, function, encoded)
  puts "Updating #{function[:function_name]}"
  lambda.update_function_code({
    function_name: function[:function_name],
    zip_file: encoded,
    publish: true
  })
end

def create(lambda, function, encoded)
  puts "Creating function:"
  pp(function)
  function[:runtime] = "java8"
  function[:publish] = true
  function[:code] = { zip_file: encoded }
  lambda.create_function(function)
end

def upload(existing, client, toCreate, encoded)
  # check to see if the function already exists
  updated = false
  existing.each{|func|
    if func.function_name.eql? toCreate[:function_name]
      update(client, toCreate, encoded)
      updated = true
      break
    end
  }
  if !updated
    create(client, toCreate, encoded)
  end
end

def getCorrectResult(msg)
  result = 'a'
  first = true
  until result.eql?("y") || result.eql?("n")
    puts "Incorrect response, please try again." unless first
    first  = false
    puts msg
    result = gets.chomp[0].downcase
  end
  result
end

def getFileStat(file)
  stat = File::Stat.new(file)
  "\nname: #{file}\n size: #{(stat.size/1000000.0).round(2)}M"+
  "\n atime: #{stat.atime}"+
  "\n birthtime: #{stat.birthtime}"
end

def printProperties(jar)
  Zip::File.open(jar) do |zip_file|
    # Find specific entry
    entry = zip_file.glob($PROP_FILE).first
    raise "No properties file (#{$PROP_FILE}) present - build the jar with build.rb" if entry.nil?
    puts "Configuration contents:"
    puts entry.get_input_stream.read
  end
end

# Options
##########

require 'optparse'
require 'ostruct'
# Defaults overridable by options
options = OpenStruct.new
options.credentials = nil
options.region = 'us-east-1'
options.names = []
options.force = false
options.verbose = false

parser = OptionParser.new do |opts|
  opts.banner = "Usage: #{file} [options]"

  opts.separator "AWS options:"
  opts.on('-c', '--credentials file', "Location of the credentials file to use.") do |s|
    options.credentials = s
  end
  opts.on('-r', '--region regionname', "Specify the region. Default: #{options.region}") do |name|
    options.region = name
  end
  opts.on('--lambda name1,name2', Array, "Comma-separated names of the lambda function(s) to deploy") do |list|
    options.names += list
  end

  opts.separator "Runtime options:"
  opts.on('-f', '--force', "Force with first acceptable jar. Default: #{options.force}") do |f|
    options.force = true;
  end
  opts.on('-v', '--verbose', "Run verbosely") do |s|
    options.verbose = true
  end
  opts.on('-h', '--help', 'Displays Help') do
      puts opts
      exit
    end
end
parser.parse!


# Check to see if a deployable artifact is present
jars = Dir["#{path}/../target/lambda-*[.]jar"]
jars.each{|jar|
  if jar.end_with? "tests.jar"
    jars.delete jar
    end
 }
raise "No deployable jar found!" unless !jars.empty?

# There are some jars, so ask the user if they want to deploy this particular jar
jar = jars[0]

unless options.force
  deployCheck = "Do you want to deploy #{getFileStat(jar)}\n? [y/n]"
  result = getCorrectResult(deployCheck)
  exit unless result.eql?("y")
end

puts "Attempting to deploy: #{jar}"
require 'rubygems'
require 'zip'
printProperties(jar) if options.verbose

# Actually do the deployment of the lambda functions
require 'aws-sdk'
require 'yaml'
creds = YAML.load(File.read(options.credentials))

# Actually create the lambda function
client = Aws::Lambda::Client.new(
  region: options.region,
  access_key_id: creds['access_key_id'],
  secret_access_key: creds['secret_access_key'],
  validate_params: true
)
uploaded = client.list_functions({})

encoded = File.binread(jar)
functions.each{|function|
  # filter out functions that don't match the expected name
  if options.names.empty? || options.names.include?(function[:function_name])
    upload(uploaded.functions, client, function, encoded)
  elsif options.verbose
    puts "Skipping function: #{function[:function_name]}"
  end
}