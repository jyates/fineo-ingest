module Files
  def pp_stat(file)
    stat = File::Stat.new(file)
    "\n name: #{file}\n size: #{(stat.size/1000000.0).round(2)}M"+
    "\n atime: #{stat.atime}"+
    "\n birthtime: #{stat.birthtime}"
  end

  def print_jar_properties(jar)
    Zip::File.open(jar) do |zip_file|
      # Find specific entry
      entry = zip_file.glob($PROP_FILE).first
      raise "No properties file (#{$PROP_FILE}) present - build the jar with build.rb" if entry.nil?
      puts "Configuration contents:"
      puts entry.get_input_stream.read
    end
  end
end
