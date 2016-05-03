require 'optparse'

class BuilderParser

  def initialize(file)
    @file = file
  end

  def parse(manager)
    parser = OptionParser.new do|opts|
      opts.banner = "Usage: #{@file} [options]"
      manager.getOpts(opts)

      # add the non-module parameters
      opts.separator ""
      opts.on("--test aws-key", "Build 'test' parameters pointing the resources keyed with the " +
      "aws-key. This is different than the aws_acceess_key, but instead just a suffix attached to" +
      "resources to identify it. Also some resources are prefixed with 'test-'") do |key|
        $config.test = key
        $config.verbose = true
      end
      opts.on("--skip-tests", "Skip running tests when building deployable jar") do |s|
        $config.skip_tests ="-DskipTests"
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

    begin
      parser.parse!
    rescue  Exception => e
      puts "Parsing failed: #{e.message}"
      puts parser
      raise e
    end
  end
end
