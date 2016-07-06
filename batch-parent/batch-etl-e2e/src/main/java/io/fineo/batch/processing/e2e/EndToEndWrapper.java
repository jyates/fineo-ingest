package io.fineo.batch.processing.e2e;

import com.beust.jcommander.JCommander;
import io.fineo.batch.processing.e2e.command.LocalSparkCommand;
import io.fineo.batch.processing.e2e.command.SparkCommand;
import io.fineo.e2e.options.LocalSchemaStoreOptions;

public class EndToEndWrapper {

  public static void main(String[] args) throws Exception {
    LocalSchemaStoreOptions store = new LocalSchemaStoreOptions();
    JCommander jc = new JCommander(new Object[]{store});
    jc.addCommand("local", new LocalSparkCommand());

    jc.parse(args);
    SparkCommand cmd =
      (SparkCommand) jc.getCommands().get(jc.getParsedCommand()).getObjects().get(0);
    cmd.run(store.getStore());
  }
}
