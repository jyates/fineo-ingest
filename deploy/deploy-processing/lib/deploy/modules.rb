module Lambda
  class Func
    attr_reader :name, :desc
    attr_accessor :handler, :role, :timeout, :memory

    def initialize(name, desc)
      @name = name
      @desc = desc
      @timeout = 40 # seconds
      @memory = 256 # MB
    end

    def to_aws_hash
      hash = {}
      hash[:function_name] = @name
      hash[:role] = @role
      hash[:description] = @desc
      hash[:handler] = @handler
      hash[:timeout] = @timeout
      hash[:memory_size] = @memory
      hash
    end
  end

  class Module
    attr_reader :home_dir, :name, :validation_project, :functions

    def initialize(home, name, validation)
      @functions = {}
      @home_dir = home
      @name = name
      @validation_project = validation
    end

    def addFunctions(*funcs)
      funcs.each{|func|
        @functions[func.name] = func
      }
    end
  end
end

module EMR
  class JobJar
    attr_reader :home_dir, :name, :validation_project
    attr_accessor :s3_bucket, :s3_location

    def initialize(home, name, validation)
      @functions = {}
      @home_dir = home
      @name = name
      @validation_project = validation
    end
  end
end
