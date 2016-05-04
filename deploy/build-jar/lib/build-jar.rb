#!/usr/bin/env ruby
# Deployment modules and their dependencies

$path = File.expand_path(File.dirname(__FILE__))
$file = File.basename(__FILE__)
$root = "#{$path}/../../.."

require_relative "build/modules/batch-processing"
require_relative "build/modules/stream-processing"
require_relative "build/arg_manager"
require_relative "build/builder_parser"

require 'ostruct'

def log
  if $config.verbose
    yield
  end
  return $config.verbose
end

def log_conf(conf_file)
  log {
    puts "Contents of properties file: "
    File.open(conf_file, "r") do |f|
      f.each_line do |line|
        puts line
      end
    end
    puts
  }
end

def build_jar(processor)
  puts "Building Jar for #{processor.class}"

  manager = ArgManager.new
  processor.getPropertyModules().each { |props|
     props.addProps(manager)
  }

  # Load the configs and parse the state
  $config = OpenStruct.new
  $config.skip = ""
  $config.test = nil
  $config.verbose = false
  $config.verbose2 = false
  BuilderParser.new($file).parse(manager)

  prefix = $config.test.nil? ? nil: "test-#{$config.test}-"
  values = manager.build(prefix)
  home = "#{$root}/#{processor.home_dir}"
  conf_dir = "#{home}/config"
  conf_file = "#{home}/config/fineo-lambda.properties"

  # create the directories if they don't exist
  Dir.mkdir(conf_dir) unless Dir.exists? conf_dir
  File.open(conf_file, 'w') do |file|
    values.each { |key, value|
     file.puts "fineo.#{key}=#{value}"
    }
  end
  log_conf conf_file

  # Build the package
  cmd="mvn -f #{home} clean install -Ddeploy #{$config.skip_tests}"
  if log { puts "Running: #{cmd}"}
    system cmd
  else
    `#{cmd}`
  end
  success = $? == 0
  puts (success ? "---> [Done - #{processor.class}]" : "-----> FAILURE - #{processor.class}!!")
  success
end

modules = {
  "stream-processing" => Streams.new,
  "batch-processing" => Batched.new
}

toLoad = modules[ARGV[0]]
raise "No matching module [#{ARGV[0]}]! Options are: #{modules.keys}" if toLoad.nil?

toLoad.getProcessors.each{|processor|
  unless build_jar(processor)
    raise "Failed on jar to build: #{processor}"
  end
}
