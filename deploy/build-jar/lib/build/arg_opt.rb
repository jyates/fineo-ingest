
class ArgOpts
  attr_accessor :key, :value, :field_rename, :desc

  def initialize(key, value, test_rename, description)
    @key = key
    @value = value
    @field_rename = test_rename
    @desc = description
  end

  def ArgOpts.simple(key, value, desc)
    ArgOpts.new(key, value, ->(prefix) { value }, desc)
  end

  def ArgOpts.prefix(key, value, desc)
    opt = ArgOpts.new(key, value, ->(prefix) { "#{prefix}#{value}" }, desc)
    opt
  end
end
