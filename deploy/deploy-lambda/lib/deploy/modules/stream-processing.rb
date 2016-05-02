class StreamProcessing

  attr_reader :home_dir, :name

  def initialize()
    @home_dir = "pipeline/stream-processing"
    @name = "stream-processing"
  end

  def functions
    [
      {
        function_name: "RawToAvro",
        description: "Convert raw JSON records to avro encoded records",
        handler: "io.fineo.lambda.handle.raw.RawStageWrapper::handle",
        role: "arn:aws:iam::766732214526:role/Lambda-Raw-To-Avro-Ingest-Role",
        timeout: 40, # seconds
        memory_size: 256 # MB
      },
      {
        function_name: "AvroToStorage",
        description: "Stores the avro-formated bytes into Dynamo and S3",
        handler: "io.fineo.lambda.handle.staged.AvroToStorageWrapper::handle",
        role: "arn:aws:iam::766732214526:role/Lambda-Dynamo-Ingest-Role",
        timeout: 40, # seconds
        memory_size: 256 # MB
      }
    ]
  end
end
