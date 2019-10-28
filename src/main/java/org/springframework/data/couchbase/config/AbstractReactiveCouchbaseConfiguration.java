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

import java.util.List;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;

import java.util.ListIterator;

import com.couchbase.client.java.env.ClusterEnvironment;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 *
 * Base class for Reactive Spring Data Couchbase configuration java config
 *
 * @author Subhashni Balakrishnan
 */
@Configuration
public abstract class AbstractReactiveCouchbaseConfiguration
        extends AbstractReactiveCouchbaseDataConfiguration implements CouchbaseConfigurer {

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
     * The name of the collection within the bucket to use.  Will
     * use defaultCollection if this is an empty string.
     */
    protected String getCollectionName() { return ""; }

    /**
     * The user of the bucket. Override the method for users in Couchbase Server 5.0+.
     *
     * @return the user name.
     */
    protected String getUsername() { return getBucketName(); }

    /**
     * The password of the bucket/User of the bucket (can be an empty string).
     *
     * @return the password of the bucket/user.
     */
    protected abstract String getPassword();

    protected boolean isEnvironmentManagedBySpring() {
        return true;
    }



    @Override
    protected CouchbaseConfigurer couchbaseConfigurer() {
        return this;
    }

    protected ClusterEnvironment getEnvironment() {
        return ClusterEnvironment.builder().build();
    }

    protected ClusterOptions getOptions() {
        return ClusterOptions.clusterOptions(getUsername(), getPassword());
    }

    /**
     * Returns the {@link Cluster} instance to connect to.
     *
     * @throws Exception on Bean construction failure.
     */
    @Override
    @Bean(destroyMethod = "disconnect", name = BeanNames.COUCHBASE_CLUSTER)
    public Cluster couchbaseCluster() throws Exception {
        ListIterator<String> it = getBootstrapHosts().listIterator();
        StringBuilder sb = new StringBuilder();
        while(it.hasNext()) {
            sb.append(it.next());
            if (it.hasNext()) {
                sb.append(",");
            }
        }

        return Cluster.connect(sb.toString(), getOptions());
    }


    /**
     * Return the {@link Collection} instance to connect to.
     *
     * @throws Exception on Bean construction failure.
     */
    @Override
    @Bean(name = BeanNames.COUCHBASE_COLLECTION)
    public Collection couchbaseClient() throws Exception {
        //@Bean method can use another @Bean method in the same @Configuration by directly invoking it
        Cluster cluster = couchbaseCluster();

        Bucket bucket = couchbaseCluster().bucket(getBucketName());
        if (getCollectionName().isEmpty()) {
            return bucket.defaultCollection();
        }
        return bucket.collection(getCollectionName());
    }

    @Override
    @Bean(name = BeanNames.COUCHBASE_BUCKET)
    public Bucket couchbaseBucket() throws Exception {
        return couchbaseCluster().bucket(getBucketName());
    }
}
