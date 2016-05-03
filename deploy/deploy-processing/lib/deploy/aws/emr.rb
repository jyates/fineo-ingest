require 'aws-sdk'
require_relative 'utils'

class EmrAws

  include InternalUtils

  def initialize(options)
    @options = options
  end

  def deploy(jar, job)
    @creds ||= load_creds
    @s3 ||= Aws::S3::Client.new(region: @options.region,
                                access_key_id: @creds['access_key_id'],
                                secret_access_key: @creds['secret_access_key'],
                                validate_params: true)
    file = "#{job.s3_location}/#{jar.name}"
    @s3.copy_object(job.s3_bucket, file, jar)
  end
end
