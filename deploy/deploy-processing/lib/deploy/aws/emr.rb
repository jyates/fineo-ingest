require 'aws-sdk'
require_relative 'utils'

class EmrAws

  include InternalUtils

  def initialize(options)
    @options = options
  end

  def deploy(jar, job)
    return unless should_deploy?(job)

    @creds ||= load_creds
    @s3 ||= Aws::S3::Resource.new(region: @options.region,
                                access_key_id: @creds['access_key_id'],
                                secret_access_key: @creds['secret_access_key'],
                                validate_params: true)
    jarPath = Pathname.new(jar)
    file = "#{job.s3_location}/#{jarPath.basename}"
    s3_full_name = "#{job.s3_bucket}/#{file}"
    puts "Uploading #{jar} \n\t -> #{s3_full_name}...." if @options.verbose

    obj = @s3.bucket(job.s3_bucket).object(file)
    # generally this throws exceptions on failure
    success = obj.upload_file(jarPath)
    # but catch it just incase there was a failure
    raise "Failed to upload #{jarPath} to #{s3_full_name}!" unless success

    puts "Uploaded #{jar} to #{file}!"
    success
  end
end
