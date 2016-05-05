require_relative "../modules"

class Batches

  BATCH_PARENT = "pipeline/batch-processing-parent"
  SNS = "#{BATCH_PARENT}/lambda-prepare/sns-handler"

  def getModules
    [SnsRemoteS3.new, SnsLocalS3.new, BatchLauncher.new, BatchProcessing.new]
  end

  class SnsRemoteS3 < Lambda::Module
    def initialize()
      super(SNS, "sns-handler", nil)
      func = Lambda::Func.new("RemoteS3BatchUploadTracker", "Track remote S3 files that are in the next batch upload")
      func.handler = "io.fineo.batch.processing.lambda.sns.remote.RemoteS3BatchUploadTracker::handle"
      func.role = "arn:aws:iam::766732214526:role/Lambda-S3-File-For-Batch"
      func.memory = 192
      addFunctions(func)
    end
  end

  class SnsLocalS3 < Lambda::Module
    def initialize()
      super(SNS, "sns-handler", nil)
      func = Lambda::Func.new("LocalS3BatchUploadTracker", "Track local S3 files that are in the next batch upload")
      func.handler = "io.fineo.batch.processing.lambda.sns.local.LocalS3BatchUploadTracker::handle"
      func.role = "arn:aws:iam::766732214526:role/Lambda-S3-File-For-Batch"
      func.memory = 192
      addFunctions(func)
    end
  end

  class BatchLauncher < Lambda::Module
    def initialize()
      super("#{BATCH_PARENT}/lambda-emr-launch", "lambda-emr-launch", nil)
      func = Lambda::Func.new("LaunchBatchProcessing", "Launch the job/cluster to do the batch processing")
      func.handler = "io.fineo.batch.processing.lambda.LaunchBatchClusterWrapper::handle"
      func.role = "arn:aws:iam::766732214526:role/Lambda-Launch-Batch-Processing"
      addFunctions(func)
    end
  end

  class BatchProcessing < EMR::JobJar
    def initialize()
      super("#{BATCH_PARENT}/batch-processing", "batch-processing", nil)
      @s3_bucket = "batch.fineo.io"
      @s3_location = "emr/deploy"
    end
  end
end
