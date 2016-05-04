#!/usr/bin/env ruby

require_relative "properties"

class Properties::Dynamo

  def initialize()
    @opts = []
    @opts << ArgOpts.simple("dynamo.region","us-east-1", 'Region for the table')
    @opts << ArgOpts.simple("dynamo.limit.retries", "3", 'Number of retries to make in the Fineo AWS wrapper')
  end

  def withSchemaStore
    @opts << ArgOpts.prefix("dynamo.schema-store", "schema-customer", 'DynamoDB schema repository table name')
    self
  end

  def withIngest
    @opts << ArgOpts.prefix("dynamo.ingest.prefix", "customer-ingest", 'DynamoDB ingest table name prefix')
    self
  end

  def withCreateTable
    @opts += [ArgOpts.simple("dynamo.limit.write", "5", 'Max amount of write units to allocate to a single table'),
             ArgOpts.simple("dynamo.limit.read", "7", 'Max amount of write units to allocate to a single table')]
    self
  end

  def addProps(manager)
    manager.addAll(@opts)
  end
end
