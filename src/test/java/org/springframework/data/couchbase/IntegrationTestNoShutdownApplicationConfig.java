package org.springframework.data.couchbase;

import java.util.Collections;
import java.util.List;

import com.couchbase.client.java.env.ClusterEnvironment;
import org.springframework.context.annotation.Bean;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.config.CouchbaseConfigurer;

/**
 * Configuration for testing no shutdown
 *
 * @author Subhashni Balakrishnan
 */
public class IntegrationTestNoShutdownApplicationConfig extends AbstractCouchbaseConfiguration {

	@Bean
	public String couchbaseAdminUser() {
		return "Administrator";
	}

	@Bean
	public String couchbaseAdminPassword() {
		return "password";
	}

	@Override
	protected List<String> getBootstrapHosts() {
		return Collections.singletonList("127.0.0.1");
	}

	@Override
	protected String getBucketName() {
		return "protected";
	}

	@Override
	protected String getPassword() {
		return "password";
	}

	@Override
	public ClusterEnvironment couchbaseEnvironment() {
		return ClusterEnvironment.builder().build();
	}

	@Override
	protected boolean isEnvironmentManagedBySpring() {
		return false;
	}

	@Override
	protected CouchbaseConfigurer couchbaseConfigurer() {
		return this;
	}
}