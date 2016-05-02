module Maven
  def runTest(opts)
    logging = opts.test_log ? "-Dtest.output.to.file=false" : ""
    debug = opts.test_debug ? "-Dmaven.failsafe.debug" : ""
    cmd = "mvn -f #{$lambda_validation} clean verify -Dvalidate #{logging} #{debug}"
    puts "Running: #{cmd}" if opts.verbose
    system cmd
  end
end
