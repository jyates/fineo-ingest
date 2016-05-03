
require_relative "../properties/dynamo"
require_relative "../properties/firehose"
require_relative "../properties/kinesis"
require_relative "modules"

class Streams
  def getProcessors
    [StreamProcessing.new]
  end

  class StreamProcessing < ProcessingModules::Module
    def initialize()
      super( "pipeline/stream-processing",
      [ Properties::Kinesis,
        Properties::Firehose,
        Properties::Dynamo.new().withSchemaStore().withIngest().withCreateTable()])
    end
  end
end
