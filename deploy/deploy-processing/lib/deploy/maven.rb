module MavenTest
  def runTest(opts, lambda)
    validator = "#{lambda.validation_project}"
    return if validator.nil?
    dir = "#{$root}/#{validator}"
    logging = opts.test_log ? "-Dtest.output.to.file=false" : ""
    debug = opts.test_debug ? "-Dmaven.failsafe.debug" : ""
    cmd = "mvn -f #{dir} clean verify -Dvalidate #{logging} #{debug}"
    puts "Running: #{cmd}" if opts.verbose
    system cmd
  end
end
