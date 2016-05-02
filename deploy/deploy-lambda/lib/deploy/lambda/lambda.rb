require 'pp'

module Lambda

  def update(lambda, function, encoded)
    puts "Updating #{function[:function_name]}"
    lambda.update_function_code({
      function_name: function[:function_name],
      zip_file: encoded,
      publish: true
    })
  end

  def create(lambda, function, encoded)
    puts "Creating function:"
    pp(function)
    function[:runtime] = "java8"
    function[:publish] = true
    function[:code] = { zip_file: encoded }
    lambda.create_function(function)
  end

  def upload(existing, client, toCreate, encoded)
    # check to see if the function already exists
    updated = false
    existing.each{|func|
      if func.function_name.eql? toCreate[:function_name]
        update(client, toCreate, encoded)
        updated = true
        break
      end
    }
    if !updated
      create(client, toCreate, encoded)
    end
  end

end
