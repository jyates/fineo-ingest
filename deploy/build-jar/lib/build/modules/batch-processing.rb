
require_relative "../properties/dynamo"
require_relative "../properties/firehose"
require_relative "modules"

class Batched
  PARENT = "pipeline/batch-processing-parent"
  def getProcessors
    [ SnsHandler.new(), LaunchBatchProcessing.new(), BatchProcessing.new() ]
  end

  class BatchProcessing < ProcessingModules::Module
    def initialize()
      super("#{PARENT}/batch-processing",
        [Properties::Firehose, Properties::Dynamo.new().withSchemaStore()])
    end
  end

  class LaunchBatchProcessing < ProcessingModules::Module
    def initialize()
      super("#{PARENT}/lambda-emr-launch", [])
    end
  end

  class SnsHandler < ProcessingModules::Module
    def initialize()
      super("#{PARENT}/lambda-prepare/sns-handler",
        [Properties::Dynamo.new().withCreateBatchManifestTable()])
    end
  end
end
