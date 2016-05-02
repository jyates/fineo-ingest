
require_relative "../properties/dynamo"
require_relative "../properties/firehose"
require_relative "../properties/kinesis"

class StreamProcessing

  attr_reader :home_dir

  def initialize()
    @home_dir = "pipeline/stream-processing"
  end

  def getPropertyModules()
    [ Properties::Kinesis,
      Properties::Firehose,
      Properties::Dynamo.new().withSchemaStore().withIngest().withCreateTable()]
  end
end
