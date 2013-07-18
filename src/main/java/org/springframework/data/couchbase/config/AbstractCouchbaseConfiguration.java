/*
 * Copyright 2013 the original author or authors.
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

import com.couchbase.client.CouchbaseClient;
import java.util.HashSet;
import java.util.Set;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.data.annotation.Persistent;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * Base class for Spring Data Couchbase configuration using JavaConfig.
 *
 * @author Michael Nitschinger
 */
@Configuration
public abstract class AbstractCouchbaseConfiguration {

  /**
   * Return the {@link CouchbaseClient} instance to connect to.
   *
   * @throws Exception on Bean construction failure.
   */
  @Bean
  public abstract CouchbaseClient couchbaseClient() throws Exception;

  /**
   * Creates a {@link CouchbaseTemplate}.
   *
   * @throws Exception on Bean construction failure.
   */
  @Bean
  public CouchbaseTemplate couchbaseTemplate() throws Exception {
    return new CouchbaseTemplate(couchbaseClient(), mappingCouchbaseConverter());
  }

  /**
   * Creates a {@link MappingCouchbaseConverter} using the configured {@link #couchbaseMappingContext}.
   *
   * @throws Exception on Bean construction failure.
   */
  @Bean
  public MappingCouchbaseConverter mappingCouchbaseConverter() throws Exception {
    return new MappingCouchbaseConverter(couchbaseMappingContext());
  }

  /**
   * Creates a {@link CouchbaseMappingContext} equipped with entity classes scanned from the mapping base package.
   *
   * @throws Exception on Bean construction failure.
   */
  @Bean
  public CouchbaseMappingContext couchbaseMappingContext() throws Exception {
    CouchbaseMappingContext mappingContext = new CouchbaseMappingContext();
    mappingContext.setInitialEntitySet(getInitialEntitySet());
    return mappingContext;
  }

  /**
   * Scans the mapping base package for classes annotated with {@link Document}.
   *
   * @throws ClassNotFoundException if intial entity sets could not be loaded.
   */
  protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {
    String basePackage = getMappingBasePackage();
    Set<Class<?>> initialEntitySet = new HashSet<Class<?>>();

    if (StringUtils.hasText(basePackage)) {
			ClassPathScanningCandidateComponentProvider componentProvider =
        new ClassPathScanningCandidateComponentProvider(false);
			componentProvider.addIncludeFilter(
        new AnnotationTypeFilter(Document.class));
			componentProvider.addIncludeFilter(
        new AnnotationTypeFilter(Persistent.class));

			for (BeanDefinition candidate :
        componentProvider.findCandidateComponents(basePackage)) {
				initialEntitySet.add(ClassUtils.forName(candidate.getBeanClassName(),
          AbstractCouchbaseConfiguration.class.getClassLoader()));
			}
    }

    return initialEntitySet;
  }

  /**
   * Return the base package to scan for mapped {@link Document}s. Will return the package name of the configuration
   * class (the concrete class, not this one here) by default.
   *
   * <p>So if you have a {@code com.acme.AppConfig} extending {@link AbstractCouchbaseConfiguration} the base package
   * will be considered {@code com.acme} unless the method is overridden to implement alternate behavior.</p>
   *
   * @return the base package to scan for mapped {@link Document} classes or {@literal null} to not enable scanning for
   *         entities.
   */
  protected String getMappingBasePackage() {
    return getClass().getPackage().getName();
  }

}
