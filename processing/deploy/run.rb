#!/usr/bin/env ruby

require 'pp'

# Constants
$PROP_FILE = "fineo-lambda.properties"
module_name = "processing"

# File properties
path =File.expand_path(File.dirname(__FILE__))
lambda = "#{path}/.."
jar_dir = "#{lambda}/target"
build_script = "#{lambda}/build.sh"
$lambda_validation = "#{path}/../lambda-validate"

# List of the functions and properties about them
functions = [
  {
    function_name: "RawToAvro",
    description: "Convert raw JSON records to avro encoded records",
    handler: "io.fineo.lambda.LambdaRawRecordToAvro::handler",
    role: "arn:aws:iam::766732214526:role/Lambda-Raw-To-Avro-Ingest-Role",
    timeout: 40, # seconds
    memory_size: 256 # MB
  },
  {
    function_name: "AvroToStorage",
    description: "Stores the avro-formated bytes into Dynamo and S3",
    handler: "io.fineo.lambda.LambdaAvroToStorage::handler",
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
  "\n name: #{file}\n size: #{(stat.size/1000000.0).round(2)}M"+
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

def runTest(opts)
  logging = opts.test_log ? "-Dtest.output.to.file=false" : ""
  cmd = "mvn -f #{$lambda_validation} clean test -DallTests #{logging}"
  puts "Running: #{cmd}" if opts.verbose
  system cmd
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
options.test = false
options.test_log = false

parser = OptionParser.new do |opts|
  opts.banner = "Usage: #{File.basename(__FILE__)} [options]"

  opts.separator "AWS options:"
  opts.on('-c', '--credentials FILE', "Location of the credentials FILE to use.") do |s|
    options.credentials = s
  end
  opts.on('-r', '--region REGIONNAME', "Specify the region. Default: #{options.region}") do |name|
    options.region = name
  end
  opts.on('--lambda FUNCTIONS', Array, "Comma-separated names of the lambda function(s) to deploy") do |list|
    options.names += list
  end

  opts.separator "Runtime options:"
  opts.on("--test [log-to-sys-out]", "Run test suite against the deployed lambda functions and "+
  "whether or not logging should be to system out. Default: test: #{options.test}, sys-out: " +
  "#{options.test_log}") do |t|
    options.test = true
    options.test_log = t if !t.nil?
  end
  opts.on('-f', '--force', "Force with first acceptable jar. Default: #{options.force}") do |f|
    options.force = true
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

puts "[Verbose mode enabled]" if options.verbose

# Check to see if a deployable artifact is present
puts "Checking #{jar_dir} for jars..." if options.verbose

jars = Dir["#{jar_dir}/#{module_name}-*.jar"]
jar = jars.find{|jar| /#{module_name}-[0-9.]+(-SNAPSHOT)?-aws.jar/=~ jar}
raise "No deployable jar found!" if jar.nil?

# There are some jars, so ask the user if they want to deploy this particular jar
unless options.force
  system "date"
  deployCheck = "Found #{getFileStat(jar)}\n---> Do you want do deploy it? [y/n]"
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
begin
  creds = YAML.load(File.read(options.credentials))
rescue
  raise "Could not read credentials file at: #{options.credentials}"
end

# Actually create the lambda function
client = Aws::Lambda::Client.new(
  region: options.region,
  access_key_id: creds['access_key_id'],
  secret_access_key: creds['secret_access_key'],
  validate_params: true
)

didUpload = false
uploaded = client.list_functions({})
encoded = File.binread(jar)
functions.each{|function|
  # filter out functions that don't match the expected name
  if options.names.empty? || options.names.include?(function[:function_name])
    upload(uploaded.functions, client, function, encoded)
    didUpload = true
  elsif options.verbose
    puts "Skipping function: #{function[:function_name]}"
  end
}

runTest(options) if didUpload && options.test
