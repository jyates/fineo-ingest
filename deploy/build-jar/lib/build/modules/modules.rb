module ProcessingModules
  class Module
    attr_reader :home_dir

    def initialize(home_dir, modules)
       @home_dir = home_dir
       @modules = modules
    end

    def getPropertyModules()
      @modules
    end
  end
end
