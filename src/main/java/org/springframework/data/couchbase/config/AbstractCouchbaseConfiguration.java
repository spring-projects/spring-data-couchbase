/*
 * Copyright 2012-2017 the original author or authors
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
import java.util.Set;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.cluster.ClusterInfo;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.data.annotation.Persistent;
import org.springframework.data.couchbase.core.*;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.couchbase.repository.config.RepositoryOperationsMapping;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * Base class for Spring Data Couchbase configuration using JavaConfig.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 * @author Stephane Nicoll
 * @author Subhashni Balakrishnan
 */
@Configuration
public abstract class AbstractCouchbaseConfiguration
        extends CouchbaseConfigurationSupport  {

  @Override
  protected CouchbaseConfigurer couchbaseConfigurer() {
    return this;
  }


  /**
   * Creates a {@link CouchbaseTemplate}.
   *
   * This uses {@link #mappingCouchbaseConverter()}, {@link #translationService()} and {@link #getDefaultConsistency()}
   * for construction.
   *
   * Additionally, it will expect injection of a {@link ClusterInfo} and a {@link Bucket} beans from the context (most
   * probably from another configuration). For a self-sufficient configuration that defines such beans, see
   * {@link AbstractCouchbaseConfiguration}.
   *
   * @throws Exception on Bean construction failure.
   */
  @Bean(name = BeanNames.COUCHBASE_TEMPLATE)
  public CouchbaseTemplate couchbaseTemplate() throws Exception {
    CouchbaseTemplate template = new CouchbaseTemplate(couchbaseConfigurer().couchbaseClusterInfo(),
            couchbaseConfigurer().couchbaseClient(), mappingCouchbaseConverter(), translationService());
    template.setDefaultConsistency(getDefaultConsistency());
    return template;
  }

  /**
   * Creates the {@link RepositoryOperationsMapping} bean which will be used by the framework to choose which
   * {@link CouchbaseOperations} should back which {@link CouchbaseRepository}.
   * Override {@link #configureRepositoryOperationsMapping(RepositoryOperationsMapping)} in order to customize this.
   *
   * @throws Exception
   */
  @Bean(name = BeanNames.COUCHBASE_OPERATIONS_MAPPING)
  public RepositoryOperationsMapping repositoryOperationsMapping(CouchbaseTemplate couchbaseTemplate) throws  Exception {
    //create a base mapping that associates all repositories to the default template
    RepositoryOperationsMapping baseMapping = new RepositoryOperationsMapping(couchbaseTemplate);
    //let the user tune it
    configureRepositoryOperationsMapping(baseMapping);
    return baseMapping;
  }

  /**
   * In order to customize the mapping between repositories/entity types to couchbase templates,
   * use the provided mapping's api (eg. in order to have different buckets backing different repositories).
   *
   * @param mapping the default mapping (will associate all repositories to the default template).
   */
  protected void configureRepositoryOperationsMapping(RepositoryOperationsMapping mapping) {
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

}