package org.springframework.data.couchbase.domain;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.auditing.DateTimeProvider;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.domain.time.AuditingDateTimeProvider;
import org.springframework.data.couchbase.repository.auditing.EnableCouchbaseAuditing;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;

@Configuration
@EnableCouchbaseRepositories
@EnableCouchbaseAuditing // this activates auditing
public class Config extends AbstractCouchbaseConfiguration {
	String bucketname = "travel-sample";
	String username = "Administrator";
	String password = "password";
	String connectionString = "127.0.0.1";

	@Override
	public String getConnectionString() {
		return connectionString;
	}

	@Override
	public String getUserName() {
		return username;
	}

	@Override
	public String getPassword() {
		return password;
	}

	@Override
	public String getBucketName() {
		return bucketname;
	}

	@Bean(name = "auditorAwareRef")
	public NaiveAuditorAware testAuditorAware() {
		return new NaiveAuditorAware();
	}

	@Bean(name = "dateTimeProviderRef")
	public DateTimeProvider testDateTimeProvider() {
		return new AuditingDateTimeProvider();
	}
}
