require_relative "modules"
require_relative "aws/lambda/lambda_aws"
require_relative "aws/emr"

module AwsUtil
  def deploy(jar, component)
    if (component.is_a? Lambda::Module)
      deploy_lambda(jar, component)
    elsif (component.is_a? EMR::JobJar)
      deploy_emr_jar(jar, component)
    else
      raise "No known method to handle #{component}!"
    end
  end

  def deploy_lambda(jar, lambda)
    @lambda ||= LambdaAws.new(@options)
    @lambda.deploy(jar, lambda)
  end

  def deploy_emr_jar(jar, job)
    @emr ||= EmrAws.new(@options)
    @emr.deploy(jar, job)
  end
end
