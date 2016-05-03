module InternalUtils
  def load_creds
    require 'yaml'
    begin
      creds = YAML.load(File.read(@options.credentials))
    rescue Exception => e
      puts "Could not read credentials file at: #{@options.credentials}"
      raise e
    end
    creds
  end
end
