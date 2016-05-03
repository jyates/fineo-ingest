module Files
  def pp_stat(file)
    stat = File::Stat.new(file)
    "\n name: #{file}\n size: #{(stat.size/1000000.0).round(2)}M"+
    "\n atime: #{stat.atime}"+
    "\n birthtime: #{stat.birthtime}"
  end

  def print_jar_properties(jar)
    require 'rubygems'
    require 'zip'

    Zip::File.open(jar) do |zip_file|
      # Find specific entry
      entry = zip_file.glob($PROP_FILE).first
      raise "No properties file (#{$PROP_FILE}) present - build the jar with build.rb" if entry.nil?
      puts "Configuration contents:"
      puts entry.get_input_stream.read
    end
  end

  def find_jar(lambdas)
      home = "#{$root}/#{lambdas.home_dir}"
      jar_dir = "#{home}/target"

      # Check to see if a deployable artifact is present
      puts "Checking #{jar_dir} for jars..." if @options.verbose
      jars = Dir["#{jar_dir}/#{lambdas.name}-*.jar"]
      jar = jars.find{|jar| /#{lambdas.name}-[0-9.]+(-SNAPSHOT)?-aws.jar/=~ jar}
      raise "No deployable jar found in '#{jar_dir}'!" if jar.nil?
      return jar
  end
end
