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

package org.springframework.data.couchbase.config;

import static com.couchbase.client.java.ClusterOptions.*;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.data.annotation.Persistent;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.SimpleCouchbaseClientFactory;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.core.convert.CouchbaseCustomConversions;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.core.convert.translation.JacksonTranslationService;
import org.springframework.data.couchbase.core.convert.translation.TranslationService;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.repository.config.ReactiveRepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.config.RepositoryOperationsMapping;
import org.springframework.data.mapping.model.CamelCaseAbbreviatingFieldNamingStrategy;
import org.springframework.data.mapping.model.FieldNamingStrategy;
import org.springframework.data.mapping.model.PropertyNameFieldNamingStrategy;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.DeserializationFeature;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JacksonTransformers;

/**
 * Base class for Spring Data Couchbase configuration using JavaConfig.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 * @author Stephane Nicoll
 * @author Subhashni Balakrishnan
 * @author Jorge Rodriguez Martin
 */
@Configuration
public abstract class AbstractCouchbaseConfiguration {

	/**
	 * The connection string which allows the SDK to connect to the cluster.
	 * <p>
	 * Note that the connection string can take many forms, in its simplest it is just a single hostname
	 * like "127.0.0.1". Please refer to the couchbase Java SDK documentation for all the different
	 * possibilities and options.
	 */
	public abstract String getConnectionString();

	/**
	 * The username of the user accessing Couchbase, configured on the cluster.
	 */
	public abstract String getUserName();

	/**
	 * The password used or the username to authenticate against the cluster.
	 */
	public abstract String getPassword();

	/**
	 * The name of the bucket that should be used (for example "travel-sample").
	 */
	public abstract String getBucketName();

	/**
	 * If a non-default scope should be used, override this method.
	 *
	 * @return the custom scope name or null if the default scope should be used (default).
	 */
	protected String getScopeName() {
		return null;
	}

	/**
	 * Allows to override the {@link Authenticator} used.
	 * <p>
	 * The default implementation uses the {@link PasswordAuthenticator} and takes the username and password from
	 * {@link #getUserName()} and {@link #getPassword()} respectively.
	 *
	 * @return the authenticator to be passed into the SDK.
	 */
	protected Authenticator authenticator() {
		return PasswordAuthenticator.create(getUserName(), getPassword());
	}

	/**
	 * The {@link CouchbaseClientFactory} provides access to the lower level SDK resources.
	 *
	 * @param couchbaseCluster the cluster reference from the SDK.
	 * @return the initialized factory.
	 */
	@Bean
	public CouchbaseClientFactory couchbaseClientFactory(final Cluster couchbaseCluster) {
		return new SimpleCouchbaseClientFactory(couchbaseCluster, getBucketName(), getScopeName());
	}

	@Bean(destroyMethod = "disconnect")
	public Cluster couchbaseCluster(ClusterEnvironment couchbaseClusterEnvironment) {
		return Cluster.connect(getConnectionString(),
				clusterOptions(authenticator()).environment(couchbaseClusterEnvironment));
	}

	@Bean(destroyMethod = "shutdown")
	public ClusterEnvironment couchbaseClusterEnvironment() {
		ClusterEnvironment.Builder builder = ClusterEnvironment.builder();
		configureEnvironment(builder);
		return builder.build();
	}

	/**
	 * Can be overridden to customize the configuration of the environment before bootstrap.
	 *
	 * @param builder the builder that can be customized.
	 */
	protected void configureEnvironment(final ClusterEnvironment.Builder builder) {

	}

	@Bean(name = BeanNames.COUCHBASE_TEMPLATE)
	public CouchbaseTemplate couchbaseTemplate(CouchbaseClientFactory couchbaseClientFactory,
			MappingCouchbaseConverter mappingCouchbaseConverter, TranslationService couchbaseTranslationService) {
		return new CouchbaseTemplate(couchbaseClientFactory, mappingCouchbaseConverter, couchbaseTranslationService);
	}

	public CouchbaseTemplate couchbaseTemplate(CouchbaseClientFactory couchbaseClientFactory,
			MappingCouchbaseConverter mappingCouchbaseConverter) {
		return couchbaseTemplate(couchbaseClientFactory, mappingCouchbaseConverter, new JacksonTranslationService());
	}

	@Bean(name = BeanNames.REACTIVE_COUCHBASE_TEMPLATE)
	public ReactiveCouchbaseTemplate reactiveCouchbaseTemplate(CouchbaseClientFactory couchbaseClientFactory,
			MappingCouchbaseConverter mappingCouchbaseConverter, TranslationService couchbaseTranslationService) {
		return new ReactiveCouchbaseTemplate(couchbaseClientFactory, mappingCouchbaseConverter,
				couchbaseTranslationService);
	}

	public ReactiveCouchbaseTemplate reactiveCouchbaseTemplate(CouchbaseClientFactory couchbaseClientFactory,
			MappingCouchbaseConverter mappingCouchbaseConverter) {
		return reactiveCouchbaseTemplate(couchbaseClientFactory, mappingCouchbaseConverter,
				new JacksonTranslationService());
	}

	@Bean(name = BeanNames.COUCHBASE_OPERATIONS_MAPPING)
	public RepositoryOperationsMapping couchbaseRepositoryOperationsMapping(CouchbaseTemplate couchbaseTemplate) {
		// create a base mapping that associates all repositories to the default template
		RepositoryOperationsMapping baseMapping = new RepositoryOperationsMapping(couchbaseTemplate);
		// let the user tune it
		configureRepositoryOperationsMapping(baseMapping);
		return baseMapping;
	}

	/**
	 * In order to customize the mapping between repositories/entity types to couchbase templates, use the provided
	 * mapping's api (eg. in order to have different buckets backing different repositories).
	 *
	 * @param mapping the default mapping (will associate all repositories to the default template).
	 */
	protected void configureRepositoryOperationsMapping(RepositoryOperationsMapping mapping) {
		// NO_OP
	}

	@Bean(name = BeanNames.REACTIVE_COUCHBASE_OPERATIONS_MAPPING)
	public ReactiveRepositoryOperationsMapping reactiveCouchbaseRepositoryOperationsMapping(
			ReactiveCouchbaseTemplate reactiveCouchbaseTemplate) {
		// create a base mapping that associates all repositories to the default template
		ReactiveRepositoryOperationsMapping baseMapping = new ReactiveRepositoryOperationsMapping(
				reactiveCouchbaseTemplate);
		// let the user tune it
		configureReactiveRepositoryOperationsMapping(baseMapping);
		return baseMapping;
	}

	/**
	 * In order to customize the mapping between repositories/entity types to couchbase templates, use the provided
	 * mapping's api (eg. in order to have different buckets backing different repositories).
	 *
	 * @param mapping the default mapping (will associate all repositories to the default template).
	 */
	protected void configureReactiveRepositoryOperationsMapping(ReactiveRepositoryOperationsMapping mapping) {
		// NO_OP
	}

	/**
	 * Scans the mapping base package for classes annotated with {@link Document}.
	 *
	 * @throws ClassNotFoundException if initial entity sets could not be loaded.
	 */
	protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {
		String basePackage = getMappingBasePackage();
		Set<Class<?>> initialEntitySet = new HashSet<>();

		if (StringUtils.hasText(basePackage)) {
			ClassPathScanningCandidateComponentProvider componentProvider = new ClassPathScanningCandidateComponentProvider(
					false);
			componentProvider.addIncludeFilter(new AnnotationTypeFilter(Document.class));
			componentProvider.addIncludeFilter(new AnnotationTypeFilter(Persistent.class));
			for (BeanDefinition candidate : componentProvider.findCandidateComponents(basePackage)) {
				initialEntitySet.add(
						ClassUtils.forName(candidate.getBeanClassName(), AbstractCouchbaseConfiguration.class.getClassLoader()));
			}
		}
		return initialEntitySet;
	}

	/**
	 * Determines the name of the field that will store the type information for complex types when using the
	 * {@link #mappingCouchbaseConverter(CouchbaseMappingContext, CouchbaseCustomConversions)}. Defaults to
	 * {@value MappingCouchbaseConverter#TYPEKEY_DEFAULT}.
	 *
	 * @see MappingCouchbaseConverter#TYPEKEY_DEFAULT
	 * @see MappingCouchbaseConverter#TYPEKEY_SYNCGATEWAY_COMPATIBLE
	 */
	public String typeKey() {
		return MappingCouchbaseConverter.TYPEKEY_DEFAULT;
	}

	/**
	 * Creates a {@link MappingCouchbaseConverter} using the configured {@link #couchbaseMappingContext}.
	 *
	 * @throws Exception on Bean construction failure.
	 */
	@Bean
	public MappingCouchbaseConverter mappingCouchbaseConverter(CouchbaseMappingContext couchbaseMappingContext,
			CouchbaseCustomConversions couchbaseCustomConversions) {
		MappingCouchbaseConverter converter = new MappingCouchbaseConverter(couchbaseMappingContext, typeKey());
		converter.setCustomConversions(couchbaseCustomConversions);
		return converter;
	}

	/**
	 * Creates a {@link TranslationService}.
	 *
	 * @return TranslationService, defaulting to JacksonTranslationService.
	 */
	@Bean
	public TranslationService couchbaseTranslationService() {
		final JacksonTranslationService jacksonTranslationService = new JacksonTranslationService();
		jacksonTranslationService.afterPropertiesSet();

		// for sdk3, we need to ask the mapper _it_ uses to ignore extra fields...
		JacksonTransformers.MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		return jacksonTranslationService;
	}

	/**
	 * Creates a {@link CouchbaseMappingContext} equipped with entity classes scanned from the mapping base package.
	 *
	 * @throws Exception on Bean construction failure.
	 */
	@Bean
	public CouchbaseMappingContext couchbaseMappingContext(CustomConversions customConversions) throws Exception {
		CouchbaseMappingContext mappingContext = new CouchbaseMappingContext();
		mappingContext.setInitialEntitySet(getInitialEntitySet());
		mappingContext.setSimpleTypeHolder(customConversions.getSimpleTypeHolder());
		mappingContext.setFieldNamingStrategy(fieldNamingStrategy());
		mappingContext.setAutoIndexCreation(autoIndexCreation());

		return mappingContext;
	}

	/**
	 * Configure whether to automatically create indices for domain types by deriving the from the entity or not.
	 */
	protected boolean autoIndexCreation() {
		return false;
	}

	/**
	 * Register custom Converters in a {@link CustomConversions} object if required. These {@link CustomConversions} will
	 * be registered with the {@link #mappingCouchbaseConverter(CouchbaseMappingContext, CouchbaseCustomConversions)} )}
	 * and {@link #couchbaseMappingContext(CustomConversions)}. Returns an empty {@link CustomConversions} instance by
	 * default.
	 *
	 * @return must not be {@literal null}.
	 */
	@Bean(name = BeanNames.COUCHBASE_CUSTOM_CONVERSIONS)
	public CustomConversions customConversions() {
		return new CouchbaseCustomConversions(Collections.emptyList());
	}

	/**
	 * Return the base package to scan for mapped {@link Document}s. Will return the package name of the configuration
	 * class (the concrete class, not this one here) by default.
	 * <p/>
	 * <p>
	 * So if you have a {@code com.acme.AppConfig} extending {@link AbstractCouchbaseConfiguration} the base package will
	 * be considered {@code com.acme} unless the method is overridden to implement alternate behavior.
	 * </p>
	 *
	 * @return the base package to scan for mapped {@link Document} classes or {@literal null} to not enable scanning for
	 *         entities.
	 */
	protected String getMappingBasePackage() {
		return getClass().getPackage().getName();
	}

	/**
	 * Set to true if field names should be abbreviated with the {@link CamelCaseAbbreviatingFieldNamingStrategy}.
	 *
	 * @return true if field names should be abbreviated, default is false.
	 */
	protected boolean abbreviateFieldNames() {
		return false;
	}

	/**
	 * Configures a {@link FieldNamingStrategy} on the {@link CouchbaseMappingContext} instance created.
	 *
	 * @return the naming strategy.
	 */
	protected FieldNamingStrategy fieldNamingStrategy() {
		return abbreviateFieldNames() ? new CamelCaseAbbreviatingFieldNamingStrategy()
				: PropertyNameFieldNamingStrategy.INSTANCE;
	}
}
