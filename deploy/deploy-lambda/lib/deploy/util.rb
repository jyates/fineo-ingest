module Util
  def correct_user_answer(msg)
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
end
