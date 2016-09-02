package io.fineo.batch.processing.lambda.sns;

import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.google.common.collect.Multimap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.fineo.batch.processing.dynamo.IngestManifest;
import io.fineo.batch.processing.dynamo.IngestManifestModule;
import io.fineo.batch.processing.lambda.sns.remote.RemoteS3BatchUploadTracker;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.dynamo.rule.BaseDynamoTableTest;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRemoteS3BatchUploadTracker extends BaseDynamoTableTest {

  @Test
  public void testLoadModuleNoEvents() throws Exception {
    SNSEvent event = mock(SNSEvent.class);
    getTracker().handleEvent(event);
  }

  @Test
  public void testSingleRemoteFile() throws Exception {
    SNSEvent event = mock(SNSEvent.class);
    SNSEvent.SNSRecord record = mock(SNSEvent.SNSRecord.class);
    when(event.getRecords()).thenReturn(asList(record));
    SNSEvent.SNS sns = mock(SNSEvent.SNS.class);
    when(record.getSNS()).thenReturn(sns);
    String orgid = "1234";
    when(sns.getSubject()).thenReturn(orgid);
    String file = "s3://some.remote.file";
    when(sns.getMessage()).thenReturn(file);

    // handleEvent the event
    getTracker().handleEvent(event);

    // read the results back out
    Multimap<String, String> files = getManifest();
    assertEquals(newHashSet(orgid), files.keySet());
    assertEquals(newArrayList(file), newArrayList(files.get(orgid)));

    Mockito.verify(event).getRecords();
    Mockito.verify(record).getSNS();
    Mockito.verify(sns).getSubject();
    Mockito.verify(sns).getMessage();
  }

  private Multimap<String, String> getManifest() {
    Injector inject = Guice.createInjector(getModules());
    return inject.getInstance(IngestManifest.class).files();
  }

  private RemoteS3BatchUploadTracker getTracker() {
    return new RemoteS3BatchUploadTracker(getModules());
  }

  private List<Module> getModules() {
    List<Module> modules = new ArrayList<>();
    modules.add(props());
    modules.addAll(getDynamoModules());
    modules.add(IngestManifestModule.createForTesting());
    return modules;
  }

  private PropertiesModule props() {
    Properties props = new Properties();
    props.put(IngestManifestModule.READ_LIMIT, "1");
    props.put(IngestManifestModule.WRITE_LIMIT, "1");
    return new PropertiesModule(props);
  }
}
