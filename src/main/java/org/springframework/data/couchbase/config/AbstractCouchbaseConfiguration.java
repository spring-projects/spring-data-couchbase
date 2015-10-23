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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.cluster.ClusterInfo;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.view.ViewQuery;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.data.annotation.Persistent;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.convert.CustomConversions;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.core.convert.translation.JacksonTranslationService;
import org.springframework.data.couchbase.core.convert.translation.TranslationService;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.core.query.Consistency;
import org.springframework.data.couchbase.core.query.N1qlPrimaryIndexed;
import org.springframework.data.couchbase.core.query.N1qlSecondaryIndexed;
import org.springframework.data.couchbase.core.query.ViewIndexed;
import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.couchbase.repository.config.RepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.support.IndexManager;
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
  protected abstract List<String> getBootstrapHosts();

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
   * Is the {@link #getEnvironment()} to be destroyed by Spring?
   *
   * @return true if Spring should destroy the environment with the context, false otherwise.
   */
  protected boolean isEnvironmentManagedBySpring() {
    return true;
  }

  /**
   * Override this method if you want a customized {@link CouchbaseEnvironment}.
   * This environment will be managed by Spring, which will call its shutdown()
   * method upon bean destruction, unless you override {@link #isEnvironmentManagedBySpring()}
   * as well to return false.
   *
   * @return a customized environment, defaults to a {@link DefaultCouchbaseEnvironment}.
   */
  protected CouchbaseEnvironment getEnvironment() {
    return DefaultCouchbaseEnvironment.create();
  }

  @Bean(destroyMethod = "shutdown", name = BeanNames.COUCHBASE_ENV)
  public CouchbaseEnvironment couchbaseEnvironment() {
    CouchbaseEnvironment env = getEnvironment();
    if (isEnvironmentManagedBySpring()) {
      return env;
    }
    return new CouchbaseEnvironmentNoShutdownProxy(env);
  }

  /**
   * Returns the {@link Cluster} instance to connect to.
   *
   * @throws Exception on Bean construction failure.
   */
  @Bean(destroyMethod = "disconnect", name = BeanNames.COUCHBASE_CLUSTER)
  public Cluster couchbaseCluster() throws Exception {
    return CouchbaseCluster.create(couchbaseEnvironment(), getBootstrapHosts());
  }

  @Bean(name = BeanNames.COUCHBASE_CLUSTER_INFO)
  public ClusterInfo couchbaseClusterInfo() throws Exception {
    return couchbaseCluster().clusterManager(getBucketName(), getBucketPassword()).info();
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
   * This uses {@link #couchbaseClusterInfo()}, {@link #couchbaseClient()}, {@link #mappingCouchbaseConverter()},
   * , {@link #translationService()} and {@link #getDefaultConsistency()} for construction.
   *
   * @throws Exception on Bean construction failure.
   */
  @Bean(name = BeanNames.COUCHBASE_TEMPLATE)
  public CouchbaseTemplate couchbaseTemplate() throws Exception {
    CouchbaseTemplate template = new CouchbaseTemplate(couchbaseClusterInfo(), couchbaseClient(), mappingCouchbaseConverter(), translationService());
    template.setDefaultConsistency(getDefaultConsistency());
    return template;
  }

  /**
   * Creates the {@link RepositoryOperationsMapping} bean which will be used by the framework to choose which
   * {@link CouchbaseOperations} should back which {@link CouchbaseRepository}.
   *
   * @throws Exception
   */
  @Bean(name = BeanNames.REPO_OPERATIONS_MAPPING)
  public RepositoryOperationsMapping repositoryOperationsMapping() throws  Exception {
    return new RepositoryOperationsMapping(couchbaseTemplate());
  }

  /**
   * Determines the name of the field that will store the type information for complex types when
   * using the {@link #mappingCouchbaseConverter()}.
   * Defaults to {@value MappingCouchbaseConverter#TYPEKEY_DEFAULT}.
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
  public MappingCouchbaseConverter mappingCouchbaseConverter() throws Exception {
    MappingCouchbaseConverter converter = new MappingCouchbaseConverter(couchbaseMappingContext(), typeKey());
    converter.setCustomConversions(customConversions());
    return converter;
  }

  /**
   * Creates a {@link TranslationService}.
   *
   * @return TranslationService, defaulting to JacksonTranslationService.
   */
  @Bean
  public TranslationService translationService() {
    final JacksonTranslationService jacksonTranslationService = new JacksonTranslationService();
    jacksonTranslationService.afterPropertiesSet();
    return jacksonTranslationService;
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
    mappingContext.setSimpleTypeHolder(customConversions().getSimpleTypeHolder());
    mappingContext.setFieldNamingStrategy(fieldNamingStrategy());
    return mappingContext;
  }

  /**
   * Register custom Converters in a {@link CustomConversions} object if required. These
   * {@link CustomConversions} will be registered with the {@link #mappingCouchbaseConverter()} and
   * {@link #couchbaseMappingContext()}. Returns an empty {@link CustomConversions} instance by default.
   *
   * @return must not be {@literal null}.
   */
  @Bean
  public CustomConversions customConversions() {
    return new CustomConversions(Collections.emptyList());
  }

  /**
   * Register an {@link IndexManager} bean that will be used to process {@link ViewIndexed},
   * {@link N1qlPrimaryIndexed} and {@link N1qlSecondaryIndexed} annotations on repositories
   * to automatically create indexes.
   * <p/>
   * If this configuration is used in a context where such automatic creations are not desired (eg.
   * you want automatic index creation in Dev but not in Prod, and this configuration is the Prod one),
   * override the bean and use the {@link IndexManager#IndexManager(boolean, boolean, boolean)} constructor.
   */
  @Bean(name = BeanNames.COUCHBASE_INDEX_MANAGER)
  public IndexManager indexManager() {
    return new IndexManager();
  }

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
   * Configures a {@link FieldNamingStrategy} on the {@link CouchbaseMappingContext} instance created.
   *
   * @return the naming strategy.
   */
  protected FieldNamingStrategy fieldNamingStrategy() {
    return abbreviateFieldNames() ? new CamelCaseAbbreviatingFieldNamingStrategy() : PropertyNameFieldNamingStrategy.INSTANCE;
  }

  /**
   * Configures the default consistency for generated {@link ViewQuery view queries}
   * and {@link N1qlQuery N1QL queries} in repositories.
   *
   * @return the {@link Consistency consistency} to apply by default on generated queries.
   */
  protected Consistency getDefaultConsistency() {
    return Consistency.DEFAULT_CONSISTENCY;
  }
}