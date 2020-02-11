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
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;

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
    public ReactiveCouchbaseTemplate reactiveCouchbaseTemplate() throws Exception {
        return new ReactiveCouchbaseTemplate(couchbaseClientFactory(), mappingCouchbaseConverter());
    }
}
