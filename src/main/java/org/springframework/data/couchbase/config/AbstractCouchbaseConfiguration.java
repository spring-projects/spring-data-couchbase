/*
 * Copyright 2012-2015 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.config;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ConfigurationCondition;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.data.annotation.Persistent;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.mapping.model.CamelCaseAbbreviatingFieldNamingStrategy;
import org.springframework.data.mapping.model.FieldNamingStrategy;
import org.springframework.data.mapping.model.PropertyNameFieldNamingStrategy;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * Base class for Spring Data Couchbase configuration using JavaConfig.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 */
@Configuration
public abstract class AbstractCouchbaseConfiguration {

	/**
	 * The list of hostnames (or IP addresses) to bootstrap from.
	 *
	 * @return the list of bootstrap hosts.
	 */
	protected abstract List<String> bootstrapHosts();

	/**
	 * The name of the bucket to connect to.
	 *
	 * @return the name of the bucket.
	 */
	protected abstract String getBucketName();

	/**
	 * The password of the bucket (can be an empty string).
	 *
	 * @return the password of the bucket.
	 */
	protected abstract String getBucketPassword();

	/**
	 * Override this method if you use Couchbase outside of the Spring context.
	 * If non-null, defines the {@link CouchbaseEnvironment} to use for connection (it is your
	 * responsibility to shutdown() it).
	 *
	 * @return a pre-existing environment managed outside of Spring, or null if instead a managed
	 * environment is to be used.
	 */
	protected CouchbaseEnvironment sharedEnvironment() {
		return null; //assume most of the time we'll create a dedicated one
	}

	/**
	 * Override this method if you want a customized {@link CouchbaseEnvironment} but only
	 * use it in the Spring context. This environment will be managed by Spring, which will
	 * call its shutdown() method upon bean destruction.
	 *
	 * @return a customized environment to be managed by Spring, defaults to a {@link DefaultCouchbaseEnvironment}.
	 */
	protected CouchbaseEnvironment managedEnvironment() {
		return DefaultCouchbaseEnvironment.create();
	}

	@Bean(destroyMethod = "shutdown", name = BeanNames.COUCHBASE_ENV)
	@Conditional(ConfigurationCondition.class)
	public CouchbaseEnvironment couchbaseEnvironment() {
		CouchbaseEnvironment env = sharedEnvironment();
		if (env != null) {
			//the shared environment shouldn't have its shutdown method called when
			//the bean is destroyed, it is the responsibility of the user to destroy it.
			return new CouchbaseEnvironmentNoShutdownProxy(env);
		} else {
			return managedEnvironment();
		}
	}

	/**
	 * Returns the {@link Cluster} instance to connect to.
	 *
	 * @throws Exception on Bean construction failure.
	 */
	@Bean(destroyMethod = "disconnect", name = BeanNames.COUCHBASE_CLUSTER)
	public Cluster couchbaseCluster() throws Exception {
		return CouchbaseCluster.create(couchbaseEnvironment(), bootstrapHosts());
	}

	/**
	 * Return the {@link Bucket} instance to connect to.
	 *
	 * @throws Exception on Bean construction failure.
	 */
	@Bean(destroyMethod = "close", name = BeanNames.COUCHBASE_BUCKET)
	public Bucket couchbaseClient() throws Exception {
		//@Bean method can use another @Bean method in the same @Configuration by directly invoking it
		return couchbaseCluster().openBucket(getBucketName(), getBucketPassword());
	}

	/**
	 * Creates a {@link CouchbaseTemplate}.
	 *
	 * @throws Exception on Bean construction failure.
	 */
	@Bean(name = BeanNames.COUCHBASE_TEMPLATE)
	public CouchbaseTemplate couchbaseTemplate() throws Exception {
		//TODO use mappingCouchbaseConverter and translationService when implemented
		return new CouchbaseTemplate(couchbaseClient());
	}

	//TODO create beans for mappingCouchbaseConverter, translationService, couchbaseMappingContext when implemented
	//TODO for mappingCouchbaseConverter, allow registering of customConversions

	/**
	 * Scans the mapping base package for classes annotated with {@link Document}.
	 *
	 * @throws ClassNotFoundException if initial entity sets could not be loaded.
	 */
	protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {
		String basePackage = getMappingBasePackage();
		Set<Class<?>> initialEntitySet = new HashSet<Class<?>>();

		if (StringUtils.hasText(basePackage)) {
			ClassPathScanningCandidateComponentProvider componentProvider = new ClassPathScanningCandidateComponentProvider(false);
			componentProvider.addIncludeFilter(new AnnotationTypeFilter(Document.class));
			componentProvider.addIncludeFilter(new AnnotationTypeFilter(Persistent.class));
			for (BeanDefinition candidate : componentProvider.findCandidateComponents(basePackage)) {
				initialEntitySet.add(ClassUtils.forName(candidate.getBeanClassName(), AbstractCouchbaseConfiguration.class.getClassLoader()));
			}
		}

		return initialEntitySet;
	}

	/**
	 * Return the base package to scan for mapped {@link Document}s. Will return the package name of the configuration
	 * class (the concrete class, not this one here) by default.
	 * <p/>
	 * <p>So if you have a {@code com.acme.AppConfig} extending {@link AbstractCouchbaseConfiguration} the base package
	 * will be considered {@code com.acme} unless the method is overridden to implement alternate behavior.</p>
	 *
	 * @return the base package to scan for mapped {@link Document} classes or {@literal null} to not enable scanning for
	 * entities.
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
	 * Configures a {@link FieldNamingStrategy} on the CouchbaseMappingContext instance created.
	 *
	 * @return the naming strategy.
	 */
	protected FieldNamingStrategy fieldNamingStrategy() {
		//TODO implement a CouchbaseMappingContext, use this method (update link in javadoc)
		return abbreviateFieldNames() ? new CamelCaseAbbreviatingFieldNamingStrategy() : PropertyNameFieldNamingStrategy.INSTANCE;
	}
}
