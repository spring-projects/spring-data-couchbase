/*
 * Copyright 2017-2019 the original author or authors.
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
package org.springframework.data.couchbase.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.core.ReactiveJavaCouchbaseTemplate;
import org.springframework.data.couchbase.repository.ReactiveCouchbaseRepository;
import org.springframework.data.couchbase.repository.config.ReactiveRepositoryOperationsMapping;
import org.springframework.data.couchbase.core.ReactiveJavaCouchbaseOperations;

/**
 * Provides beans to setup reactive repositories in SDC using {@link CouchbaseConfigurer}.
 *
 * @author Subhashni Balakrishnan
 */
@Configuration
public abstract class AbstractReactiveCouchbaseDataConfiguration extends CouchbaseConfigurationSupport {

    protected abstract CouchbaseConfigurer couchbaseConfigurer();

    /**
     * Creates a {@link ReactiveJavaCouchbaseTemplate}.
     *
     * This uses {@link #mappingCouchbaseConverter()}, {@link #translationService()} and {@link #getDefaultConsistency()}
     * for construction.
     *
     *
     * @throws Exception on Bean construction failure.
     */
    @Bean(name = BeanNames.RXJAVA1_COUCHBASE_TEMPLATE)
    public ReactiveJavaCouchbaseTemplate reactiveCouchbaseTemplate() throws Exception {
        ReactiveJavaCouchbaseTemplate template = new ReactiveJavaCouchbaseTemplate(couchbaseConfigurer().couchbaseCluster(),
                couchbaseConfigurer().couchbaseClient(), mappingCouchbaseConverter(), translationService());
        template.setDefaultConsistency(getDefaultConsistency());
        return template;
    }

    /**
     * Creates the {@link ReactiveRepositoryOperationsMapping} bean which will be used by the framework to choose which
     * {@link ReactiveJavaCouchbaseOperations} should back which {@link ReactiveCouchbaseRepository}.
     * Override {@link #configureReactiveRepositoryOperationsMapping} in order to customize this.
     *
     * @throws Exception
     */
    @Bean(name = BeanNames.REACTIVE_COUCHBASE_OPERATIONS_MAPPING)
    public ReactiveRepositoryOperationsMapping reactiveRepositoryOperationsMapping(ReactiveJavaCouchbaseTemplate couchbaseTemplate) throws  Exception {
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
}
