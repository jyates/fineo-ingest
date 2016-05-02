#!/usr/bin/env ruby

$PROP_FILE = "fineo-lambda.properties"
path = File.expand_path(File.dirname(__FILE__))
file = File.basename(__FILE__)
root = "#{path}/../../.."

require_relative 'deploy/modules/stream-processing'
require_relative 'deploy/files'
require_relative 'deploy/util'
require_relative 'deploy/lambda_parser'
require_relative 'deploy/lambda/aws'

include Files
include Util

modules = { "stream-processing" => StreamProcessing.new() }
lambdas = modules[ARGV[0]]

parsing = LambdaParser.new()
parsing.parser.parse!
options = parsing.options

puts "[Verbose mode enabled]" if options.verbose

home = "#{root}/#{lambdas.home_dir}"
jar_dir = "#{home}/target"

# Check to see if a deployable artifact is present
puts "Checking #{jar_dir} for jars..." if options.verbose
jars = Dir["#{jar_dir}/#{lambdas.name}-*.jar"]
jar = jars.find{|jar| /#{lambdas.name}-[0-9.]+(-SNAPSHOT)?-aws.jar/=~ jar}
raise "No deployable jar found!" if jar.nil?

# There are some jars, so ask the user if they want to deploy this particular jar
unless options.force
  system "date"
  deployCheck = "Found #{pp_stat(jar)}\n---> Do you want do deploy it? [y/n]"
  result = correct_user_answer(deployCheck)
  exit unless result.eql?("y")
end

puts "Attempting to deploy: #{jar}"
require 'rubygems'
require 'zip'
print_jar_properties(jar) if options.verbose

did_upload = LambdaAws.new(options).deploy
runTest(options) if didUpload && options.test
