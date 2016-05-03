require 'aws-sdk'
require 'yaml'
require_relative 'lambda'

class LambdaAws

  include Lambda

  def initialize(options)
    @options = options
  end

  def deploy(jar, functions)
    begin
      creds = YAML.load(File.read(@options.credentials))
    rescue Exception => e
      puts "Could not read credentials file at: #{@options.credentials}"
      raise e
    end

    # Actually create the lambda function
    @client ||=  Aws::Lambda::Client.new(
                    region: @options.region,
                    access_key_id: creds['access_key_id'],
                    secret_access_key: creds['secret_access_key'],
                    validate_params: true)

    didUpload = false
    uploaded = client.list_functions({})
    encoded = File.binread(jar)
    functions.each{ |function|
      # filter out functions that don't match the expected name
      if @options.names.empty? || @options.names.include?(function[:function_name])
        upload(uploaded.functions, client, function, encoded)
        didUpload = true
      elsif @options.verbose
        puts "Skipping function: #{function[:function_name]}"
      end
    }

    didUpload
  end

  def getClient

end
