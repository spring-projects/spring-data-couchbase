/*
 * Copyright 2014-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.repository.cdi;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.UnsatisfiedResolutionException;
import jakarta.enterprise.inject.spi.AfterBeanDiscovery;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.enterprise.inject.spi.ProcessBean;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.repository.cdi.CdiRepositoryBean;
import org.springframework.data.repository.cdi.CdiRepositoryExtensionSupport;

/**
 * A portable CDI extension which registers beans for Spring Data Couchbase repositories.
 * 
 * @author Mark Paluch
 */
public class CouchbaseRepositoryExtension extends CdiRepositoryExtensionSupport {

	private final Map<Set<Annotation>, Bean<CouchbaseOperations>> couchbaseOperationsMap = new HashMap<Set<Annotation>, Bean<CouchbaseOperations>>();

	/**
	 * Implementation of a an observer which checks for CouchbaseOperations beans and stores them in
	 * {@link #couchbaseOperationsMap} for later association with corresponding repository beans.
	 *
	 * @param <T> The type.
	 * @param processBean The annotated type as defined by CDI.
	 */
	@SuppressWarnings("unchecked")
	<T> void processBean(@Observes ProcessBean<T> processBean) {
		Bean<T> bean = processBean.getBean();
		for (Type type : bean.getTypes()) {
			if (type instanceof Class<?> && CouchbaseOperations.class.isAssignableFrom((Class<?>) type)) {
				couchbaseOperationsMap.put(bean.getQualifiers(), ((Bean<CouchbaseOperations>) bean));
			}
		}
	}

	/**
	 * Implementation of a an observer which registers beans to the CDI container for the detected Spring Data
	 * repositories.
	 * <p>
	 * The repository beans are associated to the CouchbaseOperations using their qualifiers.
	 *
	 * @param beanManager The BeanManager instance.
	 */

	void afterBeanDiscovery(@Observes AfterBeanDiscovery afterBeanDiscovery, BeanManager beanManager) {
		for (Map.Entry<Class<?>, Set<Annotation>> entry : getRepositoryTypes()) {

			Class<?> repositoryType = entry.getKey();
			Set<Annotation> qualifiers = entry.getValue();

			CdiRepositoryBean<?> repositoryBean = createRepositoryBean(repositoryType, qualifiers, beanManager);
			afterBeanDiscovery.addBean(repositoryBean);
			registerBean(repositoryBean);
		}
	}

	/**
	 * Creates a {@link Bean}.
	 *
	 * @param <T> The type of the repository.
	 * @param repositoryType The class representing the repository.
	 * @param beanManager The BeanManager instance.
	 * @return The bean.
	 */
	private <T> CdiRepositoryBean<T> createRepositoryBean(Class<T> repositoryType, Set<Annotation> qualifiers,
			BeanManager beanManager) {

		Bean<CouchbaseOperations> couchbaseOperationsBean = this.couchbaseOperationsMap.get(qualifiers);

		if (couchbaseOperationsBean == null) {
			throw new UnsatisfiedResolutionException(String.format("Unable to resolve a bean for '%s' with qualifiers %s.",
					CouchbaseOperations.class.getName(), qualifiers));
		}

		return new CouchbaseRepositoryBean<T>(couchbaseOperationsBean, qualifiers, repositoryType, beanManager,
				getCustomImplementationDetector());
	}
}
