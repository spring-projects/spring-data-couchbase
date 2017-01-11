/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.config;

import java.util.HashSet;
import java.util.Set;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.cluster.ClusterInfo;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.data.annotation.Persistent;
import org.springframework.data.couchbase.core.RxJavaCouchbaseTemplate;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.repository.ReactiveCouchbaseRepository;
import org.springframework.data.couchbase.repository.config.ReactiveRepositoryOperationsMapping;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * Base class for Reactive Spring Data Couchbase configuration using JavaConfig.
 *
 * @author Subhashni Balakrishnan
 */
@Configuration
public abstract class AbstractReactiveCouchbaseConfiguration extends CouchbaseConfigurationSupport {

	@Override
	protected CouchbaseConfigurer couchbaseConfigurer() {
		return this;
	}

	/**
	 * Creates a {@link ReactiveCouchbaseTemplate}.
	 *
	 * This uses {@link #mappingCouchbaseConverter()}, {@link #translationService()} and {@link #getDefaultConsistency()}
	 * for construction.
	 *
	 * Additionally, it will expect injection of a {@link ClusterInfo} and a {@link Bucket} beans from the context (most
	 * probably from another configuration). For a self-sufficient configuration that defines such beans, see
	 * {@link CouchbaseConfigurationSupport}.
	 *
	 * @throws Exception on Bean construction failure.
	 */
	@Bean(name = BeanNames.RXJAVA1_COUCHBASE_TEMPLATE)
	public RxJavaCouchbaseTemplate reactiveCouchbaseTemplate() throws Exception {
		RxJavaCouchbaseTemplate template = new RxJavaCouchbaseTemplate(couchbaseConfigurer().couchbaseClusterInfo(),
				couchbaseConfigurer().couchbaseClient(), mappingCouchbaseConverter(), translationService());
		template.setDefaultConsistency(getDefaultConsistency());
		return template;
	}

	/**
	 * Creates the {@link ReactiveRepositoryOperationsMapping} bean which will be used by the framework to choose which
	 * {@link ReactiveCouchbaseOperations} should back which {@link ReactiveCouchbaseRepository}.
	 * Override {@link #configureReactiveRepositoryOperationsMapping} in order to customize this.
	 *
	 * @throws Exception
	 */
	@Bean(name = BeanNames.REACTIVE_COUCHBASE_OPERATIONS_MAPPING)
	public ReactiveRepositoryOperationsMapping reactiveRepositoryOperationsMapping(RxJavaCouchbaseTemplate couchbaseTemplate) throws  Exception {
		//create a base mapping that associates all repositories to the default template
		ReactiveRepositoryOperationsMapping baseMapping = new ReactiveRepositoryOperationsMapping(couchbaseTemplate);
		//let the user tune it
		configureReactiveRepositoryOperationsMapping(baseMapping);
		return baseMapping;
	}


	/**
	 * In order to customize the mapping between repositories/entity types to couchbase templates,
	 * use the provided mapping's api (eg. in order to have different buckets backing different repositories).
	 *
	 * @param mapping the default mapping (will associate all repositories to the default template).
	 */
	protected void configureReactiveRepositoryOperationsMapping(ReactiveRepositoryOperationsMapping mapping) {
		//NO_OP
	}


	/**
	 * Scans the mapping base package for classes annotated with {@link Document}.
	 *
	 * @throws ClassNotFoundException if initial entity sets could not be loaded.
	 */
	@Override
	protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {
		String basePackage = getMappingBasePackage();
		Set<Class<?>> initialEntitySet = new HashSet<Class<?>>();

		if (StringUtils.hasText(basePackage)) {
			ClassPathScanningCandidateComponentProvider componentProvider = new ClassPathScanningCandidateComponentProvider(false);
			componentProvider.addIncludeFilter(new AnnotationTypeFilter(Document.class));
			componentProvider.addIncludeFilter(new AnnotationTypeFilter(Persistent.class));
			for (BeanDefinition candidate : componentProvider.findCandidateComponents(basePackage)) {
				initialEntitySet.add(ClassUtils.forName(candidate.getBeanClassName(), AbstractReactiveCouchbaseConfiguration.class.getClassLoader()));
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

}
