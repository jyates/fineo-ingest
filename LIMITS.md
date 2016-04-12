# Ingest Platform Limits

Collection of the limits of the platform, based on the architecture.

## Physical
 1. 1MB Messages
  * Firehose limits of 1MB per object with Avro increases managed by GZIPing objects
  * KNOWN BUG: if we don't compress smaller than 1MB we will never write the object
 2. API Gateway (for all customers) per account max at 500 events/sec (default limit)
 3. All orgs + metric Ids for each processing step have to fit in memory in Spark ETL

## Functionality
 2. No repeat alias names for a metric field. Metric names can be repeated, but not the field names
  * Delete management isn't tested yet, but this looks like an issue
