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

  def should_deploy?(job)
    puts "Checking if we should deploy: #{job.name}"
    ret = @options.names.empty? || @options.names.include?(job.name)
    puts " ===> Skipping: #{job.name}" unless ret
    return ret
  end
end
