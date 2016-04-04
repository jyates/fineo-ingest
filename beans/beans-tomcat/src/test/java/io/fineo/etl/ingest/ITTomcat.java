package io.fineo.etl.ingest;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.junit.Test;

import javax.ws.rs.core.MediaType;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class ITTomcat {

  @Test
  public void testPost() throws Exception {
    Client client = Client.create();
    WebResource target = client.resource("http://localhost:8080/beans-tomcat").path("handle");
    ClientResponse respose =
      target.type(MediaType.APPLICATION_JSON_TYPE).post(ClientResponse.class, "{some:text}");
    assertEquals(200, respose.getStatus());
  }
}
