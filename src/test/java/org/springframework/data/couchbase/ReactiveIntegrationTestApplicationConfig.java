package org.springframework.data.couchbase;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.java.env.ClusterEnvironment;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.config.AbstractReactiveCouchbaseConfiguration;
import org.springframework.data.couchbase.core.ReactiveJavaCouchbaseTemplate;
import org.springframework.data.couchbase.core.WriteResultChecking;
import org.springframework.data.couchbase.core.query.Consistency;

@Configuration
public class ReactiveIntegrationTestApplicationConfig extends AbstractReactiveCouchbaseConfiguration {

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
	protected ClusterEnvironment getEnvironment() {
		return ClusterEnvironment.builder().timeoutConfig(
				TimeoutConfig.builder()
					.connectTimeout(Duration.ofMillis(10000))
					.kvTimeout(Duration.ofMillis(10000))
					.queryTimeout(Duration.ofMillis(10000))
					.viewTimeout(Duration.ofMillis(10000))
		).build();
	}

	@Override
	public ReactiveJavaCouchbaseTemplate reactiveCouchbaseTemplate() throws Exception {
		ReactiveJavaCouchbaseTemplate template = super.reactiveCouchbaseTemplate();
		template.setWriteResultChecking(WriteResultChecking.LOG);
		return template;
	}

	@Override
	protected Consistency getDefaultConsistency() {
		return Consistency.READ_YOUR_OWN_WRITES;
	}
}