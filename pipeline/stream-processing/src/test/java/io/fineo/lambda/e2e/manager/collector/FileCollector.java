package io.fineo.lambda.e2e.manager.collector;

import io.fineo.test.rule.TestOutput;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static org.junit.Assert.assertTrue;

public class FileCollector implements OutputCollector {

  private final File dir;

  public FileCollector(TestOutput output) throws IOException {
    this(output.newFolder());
  }

  public FileCollector(File dir){
    this.dir = dir;
  }

  @Override
  public OutputCollector getNextLayer(String name) {
    return new FileCollector(new File(dir, name));
  }

  @Override
  public OutputStream get(String name) throws IOException {
    if(!dir.exists()){
      assertTrue("Could not make output directory: "+dir, dir.mkdirs());
    }
    File file = new File(dir, name);
    while(file.exists()){
      file = new File(dir, file.getName()+"_1");
    }
    return new FileOutputStream(file);
  }

  @Override
  public String getRoot() {
    return dir.getPath();
  }
}
