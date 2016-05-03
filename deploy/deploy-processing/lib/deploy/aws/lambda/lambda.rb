require 'pp'

module Lambda

  def update(lambda, function, encoded)
    puts "Updating #{function.name}"
    lambda.update_function_code({
      function_name: function.name,
      zip_file: encoded,
      publish: true
    })
  end

  def create(lambda, function, encoded)
    puts "Creating function:"
    pp(function)

    hash = function.to_aws_hash
    hash[:runtime] = "java8"
    hash[:publish] = true
    pp hash
    hash[:code] = { zip_file: encoded }
    lambda.create_function(hash)
  end

  def upload(existing, client, function_to_create, encoded)
    # check to see if the function already exists
    updated = false
    existing.each{|func|
      if func.function_name.eql? function_to_create.name
        update(client, function_to_create, encoded)
        updated = true
        break
      end
    }
    create(client, function_to_create, encoded) unless updated
  end

end
