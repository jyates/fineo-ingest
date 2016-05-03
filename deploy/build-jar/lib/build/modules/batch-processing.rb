
require_relative "../properties/dynamo"
require_relative "../properties/firehose"
require_relative "modules"

class Batched
  PARENT = "pipeline/batch-processing-parent"
  def getProcessors
    [ BatchProcessing.new(), SqsHandler.new() ]
  end

  class BatchProcessing < ProcessingModules::Module
    def initialize()
      super("#{PARENT}/batch-processing",
        [Properties::Firehose, Properties::Dynamo.new().withSchemaStore()])
    end
  end

  class SqsHandler < ProcessingModules::Module
    def initialize()
      super("#{PARENT}/lambda-prepare/sqs-handler", [Properties::Dynamo.new()])
    end
  end
end
