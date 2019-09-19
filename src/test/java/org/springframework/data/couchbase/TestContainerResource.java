package org.springframework.data.couchbase;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.rules.ExternalResource;
import org.testcontainers.containers.FixedHostPortGenericContainer;

/**
 * Testcontainers as external resource. It is recommended to use it as ClassRule.
 * It also does the internal reference counting, in case if the getResource is called again.
 *
 */
public class TestContainerResource extends ExternalResource {

    private static FixedHostPortGenericContainer couchbaseContainer = null;
    private static final AtomicInteger referenceCount = new AtomicInteger();
    private static TestContainerResource currentInstance;
    private static String serverVersion;


    public static TestContainerResource getResource() {
        if (currentInstance == null) {
            currentInstance = new TestContainerResource();
            try {
                Properties properties = new Properties();
                properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("server.properties"));
                serverVersion = properties.getProperty("server.version");
                if(!"container".equals(properties.getProperty("server.resource"))) {
                    return null;
                }
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            couchbaseContainer = new FixedHostPortGenericContainer("couchbase:" + serverVersion)
                    .withFixedExposedPort(8091, 8091)
                    .withFixedExposedPort(18091, 18091)
                    .withFixedExposedPort(8092, 8092)
                    .withFixedExposedPort(18092, 18092)
                    .withFixedExposedPort(8093, 8093)
                    .withFixedExposedPort(18093, 18093)
                    .withFixedExposedPort(8094, 8094)
                    .withFixedExposedPort(18094, 18094)
                    .withFixedExposedPort(11210, 11210)
                    .withFixedExposedPort(11211, 11211)
                    .withFixedExposedPort(11207, 11207);
            couchbaseContainer.waitingFor(new CouchbaseWaitStrategy());
            couchbaseContainer.start();
        }

        return currentInstance;
    }

    @Override
    protected void before() {
        referenceCount.incrementAndGet();
    }

    @Override
    protected void after() {
        if (referenceCount.decrementAndGet() == 0 && couchbaseContainer != null) {
            if(couchbaseContainer.isRunning()) {
                couchbaseContainer.close();
            }
            currentInstance = null;
        }
    }
}
