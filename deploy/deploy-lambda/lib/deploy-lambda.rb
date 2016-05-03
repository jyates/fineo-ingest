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
require_relative 'deploy/lambda/aws'

include Files

parsing = LambdaParser.new()
parsing.parser.parse!
@options = parsing.options

puts "[Verbose mode enabled]" if @options.verbose

modules = {
  "stream-processing" => Streaming.new,
  "batch-processing" => Batches.new
  }

lambdas = modules[ARGV[0]]
raise "No matching module [#{ARGV[0]}]! Options are: #{modules.keys}" if lambdas.nil?

aws = LambdaAws.new(@options)
lambdas.getModules.each{|lambda|
  jar = find_jar(lambda)
  Util::check_deploy(jar, @options.force)

  puts "Attempting to deploy: #{jar}"
  print_jar_properties(jar) if @options.verbose

  did_upload = aws.deploy(jar, lambda.functions)
  runTest(options, lambda) if didUpload && options.test
}
