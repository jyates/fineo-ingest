# Full Deployment Resources

## Kinesis
 * fineo-customer-ingest
 * fineo-parsed-records

## SNS
 * batch-ingest-files_local-s3
 * batch-ingest-files_remote

## Dynamo
 * schema-customer
 * ingest-batch-manifest
 * customer-ingest-<cwt start>_<cwt end>_<fwt month>_<fwt year>
  * cwt - client write time
  * fwt - fineo write time

## Firehose
### Raw -> Avro Lambda
 * fineo-raw-archive
 * fineo-raw-malformed
 * fineo-raw-commit-failure

### Avro -> Storage Lambda
 * fineo-staged-archive
 * fineo-staged-dynamo-error
 * fineo-staged-commit-failure

### Batch Processing
 * fineo-raw-archive (duplicate)
 * fineo-staged-archive (duplicate)

## EMR
  * periodic batch processing
  * long running read cluster
