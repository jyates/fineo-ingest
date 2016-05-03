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
