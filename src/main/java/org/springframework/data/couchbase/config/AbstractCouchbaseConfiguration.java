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

import com.couchbase.client.java.Cluster;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.SimpleCouchbaseClientFactory;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.repository.config.RepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.support.IndexManager;

/**
 * Base class for Spring Data Couchbase configuration using JavaConfig.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 * @author Stephane Nicoll
 * @author Subhashni Balakrishnan
 */
@Configuration
public abstract class AbstractCouchbaseConfiguration extends CouchbaseConfigurationSupport {

    public abstract Cluster cluster();

    public abstract String getBucketName();

    @Bean
    public CouchbaseClientFactory couchbaseClientFactory() {
        return new SimpleCouchbaseClientFactory(cluster(), getBucketName());
    }

    @Bean
    public CouchbaseTemplate couchbaseTemplate() throws Exception {
        return new CouchbaseTemplate(couchbaseClientFactory(), mappingCouchbaseConverter());
    }

    @Bean
    public IndexManager couchbaseIndexManager() {
        return new IndexManager(couchbaseClientFactory(), false, false); //this ignores view, N1QL primary and secondary annotations
    }

    @Bean
    public RepositoryOperationsMapping couchbaseRepositoryOperationsMapping(CouchbaseTemplate couchbaseTemplate) throws  Exception {
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
