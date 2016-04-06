package io.fineo.etl.ingest;

import com.github.mjeanroy.junit.servers.rules.TomcatServerRule;
import com.github.mjeanroy.junit.servers.tomcat.EmbeddedTomcat;
import com.github.mjeanroy.junit.servers.tomcat.EmbeddedTomcatConfiguration;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class ITTomcat {

  private static final Log LOG = LogFactory.getLog(ITTomcat.class);

  @ClassRule
  public static TomcatServerRule SERVER = new TomcatServerRule(new EmbeddedTomcat(
    EmbeddedTomcatConfiguration.builder()
                               .withBaseDir("target/tomcat/" + UUID.randomUUID())
                               .withWebapp("beans/beans-tomcat/src/main/webapp")
                               .withPath("")
                               .build()));

  @Test(timeout = 1000)
  public void testIsUp() throws Exception {
    Client client = Client.create();
    LOG.info("Using url: " + SERVER.getUrl());
    WebResource target = client.resource(SERVER.getUrl());
    target.get(ClientResponse.class);
  }
}
