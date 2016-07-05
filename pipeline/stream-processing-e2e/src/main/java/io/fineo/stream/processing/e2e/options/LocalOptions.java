package io.fineo.stream.processing.e2e.options;

import com.beust.jcommander.Parameter;

/**
 * Options for the location of a locally running (e.g. not in amazon) store
 */
public class LocalOptions {

  @Parameter(names = "--port", description = "Port on which dynamo is running")
  public int port = -1;

  @Parameter(names = "--host", description = "Hostname where dynamo is running")
  public String host;

  @Parameter(names = "--schema-table", description = "Table where schema information is stored")
  public String schemaTable;

  @Parameter(names = "--ingest-table-prefix", description = "Dynamo table name prefix in which to "
                                                            + "store writes")
  public String ingestTablePrefix;
}
