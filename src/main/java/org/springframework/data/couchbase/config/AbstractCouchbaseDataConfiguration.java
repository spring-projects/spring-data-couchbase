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

import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.couchbase.repository.config.RepositoryOperationsMapping;


/**
 * Provides beans to setup SDC using {@link CouchbaseConfigurer}.
 * This is used by Spring boot to provide auto configuration support.
 *
 *
 * @author Simon Baslé
 * @author Subhashni Balakrishnan
 */
@Configuration
public abstract class AbstractCouchbaseDataConfiguration extends CouchbaseConfigurationSupport {

  protected abstract CouchbaseConfigurer couchbaseConfigurer();

  /**
   * Creates a {@link CouchbaseTemplate}.
   *
   * This uses {@link #mappingCouchbaseConverter()}, {@link #translationService()} and {@link #getDefaultConsistency()}
   * for construction.
   *
   * Additionally, it will expect injection of a {@link ClusterEnvironment} and a {@link Collection} beans from the context (most
   * probably from another configuration). For a self-sufficient configuration that defines such beans, see
   * {@link AbstractCouchbaseConfiguration}.
   *
   * @throws Exception on Bean construction failure.
   */
  @Bean(name = BeanNames.COUCHBASE_TEMPLATE)
  public CouchbaseTemplate couchbaseTemplate() throws Exception {
    CouchbaseTemplate template = new CouchbaseTemplate(couchbaseConfigurer().couchbaseCluster(),
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
}
