# ETL

The is the process that is used to transform kinda-avro-encoded files in S3 into files that are
then read by Spark/Drill.

## High level architecture

Files are read from the staging directory in S3 into Spark. From there, a few things happen:

  1. Depduplication - just saves us data we have to store
  2. Field cleanup - sometimes fields come in with malformed names. We replace them with what they 'should' be based on known transforms
  3. Determine all possible fields per customer - this leads to whatever schema changes we need to support in Redshift to read the data.
  4. Write files back to S3 ready for ingest
