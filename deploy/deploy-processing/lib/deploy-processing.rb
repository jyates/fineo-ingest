#!/usr/bin/env ruby

$PROP_FILE = "fineo-lambda.properties"
path = File.expand_path(File.dirname(__FILE__))
file = File.basename(__FILE__)
$root = "#{path}/../../.."

require_relative 'deploy/modules/stream-processing'
require_relative 'deploy/modules/batch-processing'
require_relative 'deploy/files'
require_relative 'deploy/util'
require_relative 'deploy/lambda_parser'
require_relative 'deploy/aws'

include Files

parsing = LambdaParser.new()
parsing.parser.parse!
@options = parsing.options

puts "[Verbose mode enabled]" if @options.verbose

modules = {
  "stream-processing" => Streaming.new,
  "batch-processing" => Batches.new
  }

components = modules[ARGV[0]]
raise "No matching module [#{ARGV[0]}]! Options are: #{modules.keys}" if components.nil?

include AwsUtil
components.getModules.each{|component|
  jar = find_jar(component)
  Util::check_deploy(jar, @options.force)

  puts "Attempting to deploy: #{jar}"
  print_jar_properties(jar) if @options.verbose

  did_upload = deploy(jar, component)
  runTest(options, component) if didUpload && options.test
}
