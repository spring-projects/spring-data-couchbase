/*
 * Copyright 2012-2020 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.domain;

import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.domain.time.AuditingDateTimeProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.auditing.DateTimeProvider;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.repository.auditing.EnableCouchbaseAuditing;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.util.ClusterAwareIntegrationTests;

import java.lang.reflect.InvocationTargetException;

/**
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 * @since 3.0
 */
@Configuration
@EnableCouchbaseRepositories
@EnableCouchbaseAuditing
public class Config extends AbstractCouchbaseConfiguration {
	String bucketname = "travel-sample";
	String username = "Administrator";
	String password = "password";
	String connectionString = "127.0.0.1";

	// if running a clusterAwareIntegrationTests, use those properties
	static Class clusterAware = null;

	static {
		try {
			clusterAware = Class.forName("org.springframework.data.couchbase.util.ClusterAwareIntegrationTests");
		} catch (ClassNotFoundException cnfe) {
		}
	}

	@Override
	public String getConnectionString() {
		if (clusterAware != null) {
			try {
				return (String) clusterAware.getMethod("connectionString").invoke(null, (Object[]) null);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		return connectionString;
	}

	@Override
	public String getUserName() {
		if (clusterAware != null) {
			try {
				return (String) clusterAware.getMethod("username").invoke(null, (Object[]) null);
			} catch (Exception e) {				throw new RuntimeException(e);
			}
		}
		return username;
	}

	@Override
	public String getPassword() {
		if (clusterAware != null) {
			try {
				return (String) clusterAware.getMethod("password").invoke(null, (Object[]) null);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		return password;
	}

	@Override
	public String getBucketName() {
		if (clusterAware != null) {
			try {
				return (String) clusterAware.getMethod("bucketName").invoke(null, (Object[]) null);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
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
