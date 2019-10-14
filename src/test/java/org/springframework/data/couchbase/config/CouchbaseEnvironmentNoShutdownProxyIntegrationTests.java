package org.springframework.data.couchbase.config;

import com.couchbase.client.java.env.CouchbaseEnvironment;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.ContainerResourceRunner;
import org.springframework.data.couchbase.IntegrationTestNoShutdownApplicationConfig;
import org.springframework.test.context.ContextConfiguration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Simple test to make sure that environment is not shutdown if not life cycle managed by Spring.
 */
@RunWith(ContainerResourceRunner.class)
@ContextConfiguration(classes = IntegrationTestNoShutdownApplicationConfig.class)
public class CouchbaseEnvironmentNoShutdownProxyIntegrationTests {

	@Autowired
	public CouchbaseEnvironment environment;

	@Test
	public void testEnvironmentShutDown() {
		assertThat(environment.shutdown()).as("Should return false").isEqualTo(false);
	}
}
