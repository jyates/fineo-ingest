package io.fineo.etl.ingest;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;

/**
 * Servlet that hosts the transform from raw events to avro encoded messages
 */
public class RawToAvroServlet extends GuiceServletContextListener {
    @Override
    protected Injector getInjector() {
        return Guice.createInjector(new ServletModule() {
            @Override
            protected void configureServlets() {
                super.configureServlets();
                bind(String.class).toInstance("Hello, World!");
                serve("/").with(TestServlet.class);
            }
        });
    }
}
