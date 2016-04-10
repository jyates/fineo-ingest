# Redshift ETL

The is the process that is used to transform files in S3 into files that are ingested by Redshift.

## High level architecture

Files are read from the staging directory in S3 into Spark. From there, a few things happen:

  1. Depduplication - just saves us data we have to store
  2. Field cleanup - sometimes fields come in with malformed names. We replace them with what they 'should' be based on known transforms
  3. Determine all possible fields per customer - this leads to whatever schema changes we need to support in Redshift to read the data.
  4. Write files back to S3 ready for ingest
     - new schema components are written to the side and validated by users before ingest
     - new customers schemas are not validated and immediately ingested
     - includes Redshift manifest for files to ingest
  5. Deduped rows are stored in a small number of compressed files in Glacier for backup
  6. Triggers Redshift ingest via Lambda

### User Validation

If customers send us new fields, we don't want to immediately ingest them as that causes a lot of overhead to cleanup the data and schema later. Instead, we create an 'admin task' to check the new fields and determine if they are correct or need to be merged with other fields.

This should be supportable by 'drilling down' into any record that has the new field. The end result of the user's actions would lead to a new schema AND/OR a new field mapping.

If there is a new schema, we immediately trigger the schema change in Redshift.
If there is a new field mapping, we add that mapping to be used when the next ingest job runs (this is also used by the dynamo query engine to determine equivalent fields to query).

We then add the new records to the next run of the ingest process and repeat any necessary steps above.

#### Invalid data

Sometimes this data will be considered invalid. It will then be put into a Glacier bucket with a configurable expiration time per customer
