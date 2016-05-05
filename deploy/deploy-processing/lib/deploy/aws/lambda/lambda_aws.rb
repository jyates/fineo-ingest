require 'aws-sdk'
require_relative 'lambda'
require_relative '../utils'

class LambdaAws

  include Lambda
  include InternalUtils

  def initialize(options)
    @options = options
  end

  def deploy(jar, lambda)
    @creds ||= load_creds

    # Actually create the lambda function
    @client ||=  Aws::Lambda::Client.new(
                    region: @options.region,
                    access_key_id: @creds['access_key_id'],
                    secret_access_key: @creds['secret_access_key'],
                    validate_params: true)

    didUpload = false
    uploaded = @client.list_functions({})
    encoded = File.binread(jar)
    lambda.functions.each{ |name, function|
      # filter out functions that don't match the expected name
      if should_deploy?(function)
        upload(uploaded.functions, @client, function, encoded)
        didUpload = true
      end
    }

    didUpload
  end
end
