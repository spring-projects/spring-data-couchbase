package org.springframework.data.couchbase.config;

import com.couchbase.client.java.env.CouchbaseEnvironment;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.ContainerResourceRunner;
import org.springframework.data.couchbase.IntegrationTestNoShutdownApplicationConfig;
import org.springframework.test.context.ContextConfiguration;

/**
 * Simple test to make sure that environment is not shutdown if not life cycle managed by Spring.
 */
@RunWith(ContainerResourceRunner.class)
@ContextConfiguration(classes = IntegrationTestNoShutdownApplicationConfig.class)
public class CouchbaseEnvironmentNoShutdownProxyTest {

	@Autowired
	public CouchbaseEnvironment environment;

	@Test
	public void testEnvironmentShutDown() {
		Assert.assertEquals("Should return false", false, environment.shutdown());
	}
}
