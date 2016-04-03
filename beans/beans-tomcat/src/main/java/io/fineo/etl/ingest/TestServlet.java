package io.fineo.etl.ingest;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@Singleton
public class TestServlet extends HttpServlet {
    private static final long serialVersionUID = 7528373021106530918L;

    private String greeting;

    @Inject
    public void setGreeting(String greeting) {
        this.greeting = greeting;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        resp.getOutputStream().print(greeting);
    }
}
