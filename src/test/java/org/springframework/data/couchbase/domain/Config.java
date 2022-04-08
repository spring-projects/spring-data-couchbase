/*
 * Copyright 2012-2021 the original author or authors
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

import java.lang.reflect.InvocationTargetException;

import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.auditing.DateTimeProvider;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.SimpleCouchbaseClientFactory;
import org.springframework.data.couchbase.cache.CouchbaseCacheManager;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.core.convert.CouchbaseCustomConversions;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.core.convert.translation.JacksonTranslationService;
import org.springframework.data.couchbase.core.convert.translation.TranslationService;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.domain.time.AuditingDateTimeProvider;
import org.springframework.data.couchbase.repository.auditing.EnableCouchbaseAuditing;
import org.springframework.data.couchbase.repository.auditing.EnableReactiveCouchbaseAuditing;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.repository.config.EnableReactiveCouchbaseRepositories;
import org.springframework.data.couchbase.repository.config.ReactiveRepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.config.RepositoryOperationsMapping;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.DeserializationFeature;
import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JacksonTransformers;

/**
 * @author Michael Nitschinger
 * @author Michael Reiche
 * @author Jorge Rodriguez Martin
 * @since 3.0
 */
@Configuration
@EnableCouchbaseRepositories
@EnableReactiveCouchbaseRepositories
@EnableCouchbaseAuditing(dateTimeProviderRef = "dateTimeProviderRef")
@EnableReactiveCouchbaseAuditing(dateTimeProviderRef = "dateTimeProviderRef")
@EnableCaching
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
			if (clusterAware.getMethod("config").invoke(null) == null)
				clusterAware = null;
		} catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
			e.printStackTrace();
		}
	}

	String clusterGet(String methodName, String defaultValue) {
		if (clusterAware != null) {
			try {
				return (String) clusterAware.getMethod(methodName).invoke(null);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		return defaultValue;
	}

	@Override
	public String getConnectionString() {
		return clusterGet("connectionString", connectionString);
	}

	@Override
	public String getUserName() {
		return clusterGet("username", username);
	}

	@Override
	public String getPassword() {
		return clusterGet("password", password);
	}

	@Override
	public String getBucketName() {
		return clusterGet("bucketName", bucketname);
	}

	@Override
	protected void configureEnvironment(ClusterEnvironment.Builder builder) {
		if (getConnectionString().contains("cloud.couchbase.com")) {
			builder.securityConfig(
					SecurityConfig.builder().trustManagerFactory(InsecureTrustManagerFactory.INSTANCE).enableTls(true));
		}
	}

	@Bean(name = "auditorAwareRef")
	public NaiveAuditorAware testAuditorAware() {
		return new NaiveAuditorAware();
	}

	@Bean(name = "reactiveAuditorAwareRef")
	public ReactiveNaiveAuditorAware testReactiveAuditorAware() {
		return new ReactiveNaiveAuditorAware();
	}

	@Bean(name = "dateTimeProviderRef")
	public DateTimeProvider testDateTimeProvider() {
		return new AuditingDateTimeProvider();
	}

	@Override
	public void configureReactiveRepositoryOperationsMapping(ReactiveRepositoryOperationsMapping baseMapping) {
		try {
			// comment out references to 'protected' and 'mybucket' - they are only to show how multi-bucket would work
			// ReactiveCouchbaseTemplate personTemplate = myReactiveCouchbaseTemplate(myCouchbaseClientFactory("protected"),
			// (MappingCouchbaseConverter) (baseMapping.getDefault().getConverter()));
			// baseMapping.mapEntity(Person.class, personTemplate); // Person goes in "protected" bucket
			// ReactiveCouchbaseTemplate userTemplate = myReactiveCouchbaseTemplate(myCouchbaseClientFactory("mybucket"),
			// (MappingCouchbaseConverter) (baseMapping.getDefault().getConverter()));
			// baseMapping.mapEntity(User.class, userTemplate); // User goes in "mybucket"
			// everything else goes in getBucketName() ( which is travel-sample )
		} catch (Exception e) {
			throw e;
		}
	}

	@Override
	public void configureRepositoryOperationsMapping(RepositoryOperationsMapping baseMapping) {
		try {
			// comment out references to 'protected' and 'mybucket' - they are only to show how multi-bucket would work
			// CouchbaseTemplate personTemplate = myCouchbaseTemplate(myCouchbaseClientFactory("protected"),
			// (MappingCouchbaseConverter) (baseMapping.getDefault().getConverter()));
			// baseMapping.mapEntity(Person.class, personTemplate); // Person goes in "protected" bucket
			// MappingCouchbaseConverter cvtr = (MappingCouchbaseConverter)baseMapping.getDefault().getConverter();
			// CouchbaseTemplate userTemplate = myCouchbaseTemplate(myCouchbaseClientFactory("mybucket"),
			// (MappingCouchbaseConverter) (baseMapping.getDefault().getConverter()));
			// baseMapping.mapEntity(User.class, userTemplate); // User goes in "mybucket"
			// everything else goes in getBucketName() ( which is travel-sample )
		} catch (Exception e) {
			throw e;
		}
	}

	// do not use reactiveCouchbaseTemplate for the name of this method, otherwise the value of that bean
	// will be used instead of the result of this call (the client factory arg is different)
	public ReactiveCouchbaseTemplate myReactiveCouchbaseTemplate(CouchbaseClientFactory couchbaseClientFactory,
			MappingCouchbaseConverter mappingCouchbaseConverter) {
		return new ReactiveCouchbaseTemplate(couchbaseClientFactory, mappingCouchbaseConverter,
				new JacksonTranslationService(), getDefaultConsistency());
	}

	// do not use couchbaseTemplate for the name of this method, otherwise the value of that been
	// will be used instead of the result from this call (the client factory arg is different)
	public CouchbaseTemplate myCouchbaseTemplate(CouchbaseClientFactory couchbaseClientFactory,
			MappingCouchbaseConverter mappingCouchbaseConverter) {
		return new CouchbaseTemplate(couchbaseClientFactory, mappingCouchbaseConverter, new JacksonTranslationService(),
				getDefaultConsistency());
	}

	// do not use couchbaseClientFactory for the name of this method, otherwise the value of that bean will
	// will be used instead of this call being made ( bucketname is an arg here, instead of using bucketName() )
	public CouchbaseClientFactory myCouchbaseClientFactory(String bucketName) {
		return new SimpleCouchbaseClientFactory(getConnectionString(), authenticator(), bucketName);
	}

	// convenience constructor for tests
	public MappingCouchbaseConverter mappingCouchbaseConverter() {
		MappingCouchbaseConverter converter = null;
		try {
			// MappingCouchbaseConverter relies on a SimpleInformationMapper
			// that has an getAliasFor(info) that just returns getType().getName().
			// Our CustomMappingCouchbaseConverter uses a TypeBasedCouchbaseTypeMapper that will
			// use the DocumentType annotation
			converter = new CustomMappingCouchbaseConverter(couchbaseMappingContext(customConversions()), typeKey());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return converter;
	}

	/* This uses a CustomMappingCouchbaseConverter instead of MappingCouchbaseConverter */
	@Override
	@Bean(name = "mappingCouchbaseConverter")
	public MappingCouchbaseConverter mappingCouchbaseConverter(CouchbaseMappingContext couchbaseMappingContext,
			CouchbaseCustomConversions couchbaseCustomConversions /* there is a customConversions() method bean  */) {
		// MappingCouchbaseConverter relies on a SimpleInformationMapper
		// that has an getAliasFor(info) that just returns getType().getName().
		// Our CustomMappingCouchbaseConverter uses a TypeBasedCouchbaseTypeMapper that will
		// use the DocumentType annotation
		MappingCouchbaseConverter converter = new CustomMappingCouchbaseConverter(couchbaseMappingContext, typeKey());
		converter.setCustomConversions(couchbaseCustomConversions);
		return converter;
	}

	@Override
	@Bean(name = "couchbaseTranslationService")
	public TranslationService couchbaseTranslationService() {
		final JacksonTranslationService jacksonTranslationService = new JacksonTranslationService();
		jacksonTranslationService.afterPropertiesSet();

		// for sdk3, we need to ask the mapper _it_ uses to ignore extra fields...
		JacksonTransformers.MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		return jacksonTranslationService;
	}

	@Bean
	public CouchbaseCacheManager cacheManager(CouchbaseTemplate couchbaseTemplate) throws Exception {
		CouchbaseCacheManager.CouchbaseCacheManagerBuilder builder = CouchbaseCacheManager.CouchbaseCacheManagerBuilder
				.fromConnectionFactory(couchbaseTemplate.getCouchbaseClientFactory());
		return builder.build();
	}

	@Override
	public String typeKey() {
		return "t"; // this will override '_class', is passed in to new CustomMappingCouchbaseConverter
	}

	static String scopeName = null;

	@Override
	protected String getScopeName() {
		return scopeName;
	}

	public static void setScopeName(String scopeName) {
		Config.scopeName = scopeName;
	}

}
