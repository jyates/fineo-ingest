
require_relative "../properties/dynamo"
require_relative "../properties/firehose"

class BatchProcessing

  attr_reader :home_dir

  def initialize()
    @home_dir = "pipeline/batch-processing-parent/batch-processing"
  end

  def getPropertyModules()
    return [Properties::Firehose, Properties::Dynamo.new().withSchemaStore()]
  end
end
