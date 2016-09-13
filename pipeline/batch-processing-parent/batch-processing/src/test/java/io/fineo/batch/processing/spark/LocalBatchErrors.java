package io.fineo.batch.processing.spark;

import io.fineo.batch.processing.spark.options.BatchOptions;

import java.io.File;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 */
public class LocalBatchErrors {

  private final String base;

  public LocalBatchErrors(String base) {
    this.base = base;
  }

  public List<Instant> getRuns() {
    File versionDir = runs();
    return Arrays.asList(versionDir.list()).stream()
                 .map(text -> Instant.ofEpochMilli(Long.valueOf(text)))
                 .collect(Collectors.toList());
  }

  public List<OrgErrors> getErrors(Instant run) {
    File runs = new File(base, BatchOptions.VERSION);
    File ran = new File(runs, Long.toString(run.toEpochMilli()));
    File[] orgs = ran.listFiles();
    List<OrgErrors> errors = new ArrayList<>();
    for (File orgDir : orgs) {
      OrgErrors oe = new OrgErrors(orgDir.getName());
      errors.add(oe);
      File[] files = orgDir.listFiles();
      for (File f : files) {
        if (f.length() > 0 && !f.getName().startsWith(".")) {
          oe.add(f);
        }
      }
    }
    return errors;
  }

  private File runs() {
    return new File(base, BatchOptions.VERSION);
  }

  public static class OrgErrors {
    private final String orgId;
    private final List<File> errors = new ArrayList<>();

    public OrgErrors(String orgId) {
      this.orgId = orgId;
    }

    public String getOrgId() {
      return orgId;
    }

    public List<File> getErrors() {
      return errors;
    }

    public void add(File f) {
      this.errors.add(f);
    }
  }
}
