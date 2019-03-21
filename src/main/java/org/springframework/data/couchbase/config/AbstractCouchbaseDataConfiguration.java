/*
 * Copyright 2012-2019 the original author or authors
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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.cluster.ClusterInfo;
import com.couchbase.client.java.env.CouchbaseEnvironment;
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

@Configuration
public abstract class AbstractCouchbaseDataConfiguration {

  protected abstract CouchbaseConfigurer couchbaseConfigurer();

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
  @Bean(name = BeanNames.COUCHBASE_MAPPING_CONVERTER)
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
  @Bean(name = BeanNames.COUCHBASE_TRANSLATION_SERVICE)
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
  @Bean(name = BeanNames.COUCHBASE_MAPPING_CONTEXT)
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
  @Bean(name = BeanNames.COUCHBASE_CUSTOM_CONVERSIONS)
  public CustomConversions customConversions() {
    return new CustomConversions(Collections.emptyList());
  }

  /**
   * Register an {@link IndexManager} bean that will be used to process {@link ViewIndexed},
   * {@link N1qlPrimaryIndexed} and {@link N1qlSecondaryIndexed} annotations on repositories
   * to automatically create indexes. By default, since such automatic creations are discouraged in
   * production envrironment, the configuration will assume the worst and will ignore these annotations.
   * <p/>
   * If you are sure this configuration used in a context where such automatic creations are desired (eg.
   * you want automatic index creation in Dev, just not in Prod, and this configuration is the Dev one),
   * override the bean and use the {@link IndexManager#IndexManager()} constructor (or
   * {@link IndexManager#IndexManager(boolean, boolean, boolean)} constructor with appropriate flags set to true to
   * activate).
   */
  @Bean(name = BeanNames.COUCHBASE_INDEX_MANAGER)
  public IndexManager indexManager() {
    return new IndexManager(false, false, false); //this ignores view, N1QL primary and secondary annotations
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
