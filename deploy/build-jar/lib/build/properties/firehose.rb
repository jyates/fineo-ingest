#!/usr/bin/env ruby

require_relative "properties"

class Properties::Firehose

  ERROR_FIREHOSES = ->(test_prefix){ "#{test_prefix}failed-records" }

  def self.addProps(manager)
    manager.add(ArgOpts.simple(name("url"), "https://firehose.us-east-1.amazonaws.com", 'Firehose Url'),
      # raw record archiving
      ArgOpts.prefix(name("raw.archive"), "fineo-raw-archive", 'Name of Firehose stream to store all raw records'),
      ArgOpts.new(name("raw.error"),"fineo-raw-malformed", ERROR_FIREHOSES, 'Malformed event Kinesis Firehose stream name'),
      ArgOpts.new(name("raw.error.commit"),"fineo-raw-commit-failure", ERROR_FIREHOSES, 'Error on write event Kinesis Firehose stream name'),
      # parsed record - "staged" - params
      ArgOpts.prefix(name("staged.archive"), "fineo-staged-archive", 'Name of Firehose stream to archive all staged records'),
      ArgOpts.new(name("staged.error"), "fineo-staged-dynamo-error", ERROR_FIREHOSES, 'Malformed Avro event Kinesis Firehose stream name'),
      ArgOpts.new(name("staged.error.commit"), "fineo-staged-commit-failure", ERROR_FIREHOSES, 'Error on write Avro event Kinesis Firehose stream name'))
  end

  private

  def self.name(suffix)
    "firehose.#{suffix}"
  end
end
