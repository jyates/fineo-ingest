package io.fineo.batch.processing;

import com.beust.jcommander.Parameter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class DataExpanderOptions implements Serializable{

  @Parameter(names = {"-p", "--path"},
             description = "Full URI path to the file to expand. REPEATABLE",
             variableArity = true)
  List<String> paths = new ArrayList<>();

  @Parameter(names = "--field",
             description = "Field to use when determining the field range")
  String intervalField;

  @Parameter(names = "--field-type",
             description = "Type of the field used for the range")
  String fieldType;

  @Parameter(names = "--start", description = "Start time from the epoch")
  long start;

  @Parameter(names = "--end", description = "End time, from the epoch")
  long end;

  @Parameter(names = "--interval", description = "ms between events")
  long interval;

  @Parameter(names = "--time-field", description = "Field that describes the timestamp")
  String timeField;

  @Parameter(names = "--time-format",
             description = "Java pattern to translate the field into a timestamp string")
  String timeformat;

  @Parameter(names = "--chunks", description = "Number of chunks to use when parallelizing")
  int chunks = 100;

  @Parameter(names = {"-o", "--output"},
             description = "Full specification of the directory to which the output files "
                           + "should be written")
  String output;

}
