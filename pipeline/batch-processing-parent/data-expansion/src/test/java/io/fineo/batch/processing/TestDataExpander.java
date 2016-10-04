package io.fineo.batch.processing;

import io.fineo.spark.rule.LocalSparkRule;
import io.fineo.test.rule.TestOutput;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;


public class TestDataExpander {

  @ClassRule
  public static TestOutput tmp = new TestOutput(false);

  @ClassRule
  public static LocalSparkRule spark = new LocalSparkRule();

  @Rule
  public TestName name = new TestName();

  @Test
  public void testExpandOneRow() throws Exception {
    String timestamp = "timestamp", field1 = "f1";
    // write a simple csv file
    File csv = tmp.newFile();
    CSVFormat format = CSVFormat.DEFAULT
      .withAllowMissingColumnNames(true)
      .withHeader(timestamp, field1)
      .withSkipHeaderRecord(false);
    try (FileWriter writer = new FileWriter(csv);
         CSVPrinter printer = new CSVPrinter(writer, format)) {
      printer.printRecord(Instant.now(), 1);
    }

    File out = new File(tmp.newFolder(), name.getMethodName() + "-output");

    // setup the options
    DataExpanderOptions opts = new DataExpanderOptions();
    opts.paths.add(csv.getPath());
    opts.output = out.getAbsolutePath();
    opts.fieldType = "integer";
    opts.intervalField = field1;
    opts.timeField = timestamp;
    opts.timeformat = "uuuu-MM-dd HH:mm:ss.SSS'T'ZZZ";
    opts.interval = Duration.ofMinutes(15).toMillis();
    // should be a single row - range from 0 to 15 with 15 minute interval
    opts.start = ZonedDateTime.of(2016, 02, 10, 11, 0, 0, 135000, pst())
                              .toInstant().toEpochMilli();
    opts.end = ZonedDateTime.of(2016, 02, 10, 11, 15, 0, 0, pst())
                              .toInstant().toEpochMilli();
    DataExpander expander = new DataExpander();

    // run the job
    expander.run(spark.jsc(), opts);

    // validate the output csv
    Map<String, String> expected = new HashMap<>();
    expected.put(timestamp, "2016-02-10 11:00:00.000TPST");
    expected.put(field1, "1");

    // find all the output files in order
    List<String> files = Arrays.asList(out.list()).stream()
                               .filter(name -> name.startsWith("part-"))
                               .sorted((f1, f2) -> {
                                 int i1 = Integer.valueOf(f1.replace("part-", ""));
                                 int i2 = Integer.valueOf(f2.replace("part-", ""));
                                 return Integer.compare(i1, i2);
                               }).collect(Collectors.toList());
    List<Map<String, String>> rows = new ArrayList<>();
    rows.add(expected);
    for (String name : files) {
      File file = new File(out, name);
      try (FileReader reader = new FileReader(file)) {
        for (CSVRecord record : format.parse(reader)) {
          assertEquals("Mimatch for file: " + file, rows.remove(0), record.toMap());
        }
      }
    }
  }

  private ZoneId pst() {
    return TimeZone.getTimeZone("PST").toZoneId();
  }
}
