module Util
  def self.correct_user_answer(msg)
    result = 'a'
    first = true
    until result.eql?("y") || result.eql?("n")
      puts "Incorrect response, please try again." unless first
      first  = false
      puts msg
      result = gets.chomp[0].downcase
    end
    result
  end

  def self.check_deploy(jar, force)
      require_relative 'files'
      # There are some jars, so ask the user if they want to deploy this particular jar
      unless force
        system "date"
        deployCheck = "Found #{Files::pp_stat(jar)}\n---> Do you want do deploy it? [y/n]"
        result = correct_user_answer(deployCheck)
        exit unless result.eql?("y")
      end
  end
end
