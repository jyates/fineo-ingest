require 'ostruct'
require 'optparse'

class LambdaParser

  attr_reader :options

  def initialize
    # Defaults overridable by options
    @options = OpenStruct.new
    @options.credentials = nil
    @options.region = 'us-east-1'
    @options.names = []
    @options.force = false
    @options.verbose = false
    @options.test = false
    @options.test_log = false
  end

  def parser
    parser = OptionParser.new do |opts|
      opts.banner = "Usage: #{File.basename(__FILE__)} [lambda-set] [options]"

      opts.separator "AWS options:"
      opts.on('-c', '--credentials FILE', "Location of the credentials FILE to use.") do |s|
        @options.credentials = s
      end
      opts.on('-r', '--region REGIONNAME', "Specify the region. Default: #{@options.region}") do |name|
        @options.region = name
      end
      opts.on('--modules MODULES', Array, "Comma-separated names of the modules (lambda, job jar, etc) to deploy") do |list|
        @options.names += list
      end

      opts.separator "Runtime options:"
      opts.on("--test [log-to-sys-out]", "Run test suite against the deployed lambda functions and "+
      "whether or not logging should be to system out. Default: test: #{@options.test}, sys-out: " +
      "#{@options.test_log}") do |t|
        @options.test = true
        @options.test_log = t if !t.nil?
      end
      opts.on("--test-debug", "If the junit test should wait for a debugger to attach") do |s|
        @options.test_debug = true
      end
      opts.on('-f', '--force', "Force with first acceptable jar. Default: #{@options.force}") do |f|
        @options.force = true
      end
      opts.on('-v', '--verbose', "Run verbosely") do |s|
        @options.verbose = true
      end
      opts.on('-h', '--help', 'Displays Help') do
          puts opts
          exit
      end
    end

    parser
    end
end
