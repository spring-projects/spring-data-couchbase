/**
 * Copyright (C) 2009-2012 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package com.couchbase.spring.config;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.spring.core.CouchbaseMappingContext;
import com.couchbase.spring.core.CouchbaseTemplate;
import com.couchbase.spring.core.convert.MappingCouchbaseConverter;
import com.couchbase.spring.core.mapping.Document;
import java.util.HashSet;
import java.util.Set;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.data.annotation.Persistent;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * Base class for Spring Data Couchbase configuration using JavaConfig.
 */
@Configuration
public abstract class AbstractCouchbaseConfiguration {

  /**
   * Return the {@link CouchbaseClient} instance to connect to.
   */
  @Bean
  public abstract CouchbaseClient couchbaseClient() throws Exception;

  /**
   * Creates a {@link CouchbaseTemplate}.
   */
  @Bean
  public CouchbaseTemplate couchbaseTemplate() throws Exception {
    return new CouchbaseTemplate(couchbaseClient(), mappingCouchbaseConverter());
  }

  /**
   * Creates a {@link MappingCouchbaseConverter} using the configured {@link
   * #couchbaseMappingContext}.
   */
  @Bean
  public MappingCouchbaseConverter mappingCouchbaseConverter() throws Exception {
    return new MappingCouchbaseConverter(couchbaseMappingContext());
  }

  /**
   * Creates a {@link CouchbaseMappingContext} equipped with entity classes
   * scanned from the mapping base package.
   */
  @Bean
  public CouchbaseMappingContext couchbaseMappingContext() throws Exception {
    CouchbaseMappingContext mappingContext = new CouchbaseMappingContext();
    mappingContext.setInitialEntitySet(getInitialEntitySet());
    return mappingContext;
  }

  /**
   * Scans the mapping base package for classes annotated with {@link Document}.
   */
  protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {
    String basePackage = getMappingBasePackage();
    Set<Class<?>> initialEntitySet = new HashSet<Class<?>>();

    if(StringUtils.hasText(basePackage)) {
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
   * Return the base package to scan for mapped {@link Document}s. Will return
   * the package name of the configuration class (the concrete class, not this
   * one here) by default. So if you have a {@code com.acme.AppConfig} extending
   * {@link AbstractCouchbaseConfiguration} the base package will be considered
   * {@code com.acme} unless the method is overridden to implement alternate
   * behavior.
   *
   * @return the base package to scan for mapped {@link Document} classes or
   *         {@literal null} to not enable scanning for entities.
   */
  protected String getMappingBasePackage() {
    return getClass().getPackage().getName();
  }

}
