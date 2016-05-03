require_relative "../modules"

class Streaming
  def getModules
    return [StreamProcessing.new()]
  end

  class StreamProcessing < Lambda::Module
    def initialize()
      super("pipeline/stream-processing", "stream-processing", "pipeline/lambda-validate")

      raw = Lambda::Func.new("RawToAvro", "Convert raw JSON records to avro encoded records")
      raw.handler = "io.fineo.lambda.handle.raw.RawStageWrapper::handle"
      raw.role = "arn:aws:iam::766732214526:role/Lambda-Raw-To-Avro-Ingest-Role"

      storage = Lambda::Func.new("AvroToStorage", "Stores the avro-formated bytes into Dynamo and S3")
      storage.handler = "io.fineo.lambda.handle.staged.AvroToStorageWrapper::handle"
      storage.role = "arn:aws:iam::766732214526:role/Lambda-Dynamo-Ingest-Role"
      addFunctions raw, storage
    end
  end
end
