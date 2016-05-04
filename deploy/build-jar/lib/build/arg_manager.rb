
class ArgManager

  def initialize()
    @opts = []
    @values = {}
  end

  def add(*opt)
    @opts+= opt
  end

  def addAll(opt)
     @opts += opt
  end

  def build(test_prefix)
    @opts.each{|opt|
      unless test_prefix.nil?
        value = opt.field_rename.call(test_prefix)
      else
        value = opt.value
      end
      # set the value, if we don't have one already in our hash
      @values[opt.key] ||= value
    }
    @values["integration.test.prefix"] = test_prefix unless test_prefix.nil?
    @values
  end

  def getOpts(parser)
     @opts.each{ |opt|
        key = opt.key.gsub(".", "-")
        parser.on("--#{key} <value>", "#{opt.desc}. DEFAULT: #{opt.value}") do |name|
          @values[opt.key] = name
        end
     }
  end
end
