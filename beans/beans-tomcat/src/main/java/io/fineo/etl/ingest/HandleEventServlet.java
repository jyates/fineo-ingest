package io.fineo.etl.ingest;

import com.google.inject.Singleton;

import javax.servlet.http.HttpServlet;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

@Singleton
@Path("/handle")
public class HandleEventServlet extends HttpServlet {
  private static final long serialVersionUID = 7528373021106530918L;

  @POST
  @Consumes("application/json")
  public void handle(String body) {
  }
}
