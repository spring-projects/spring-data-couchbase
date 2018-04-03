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

import java.util.List;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.cluster.ClusterInfo;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Base class for Spring Data Couchbase configuration using JavaConfig.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 * @author Stephane Nicoll
 */
@Configuration
public abstract class AbstractCouchbaseConfiguration
        extends AbstractCouchbaseDataConfiguration implements CouchbaseConfigurer {

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

  @Override
  protected CouchbaseConfigurer couchbaseConfigurer() {
    return this;
  }

  @Override
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
  @Override
  @Bean(destroyMethod = "disconnect", name = BeanNames.COUCHBASE_CLUSTER)
  public Cluster couchbaseCluster() throws Exception {
    return CouchbaseCluster.create(couchbaseEnvironment(), getBootstrapHosts());
  }

  @Override
  @Bean(name = BeanNames.COUCHBASE_CLUSTER_INFO)
  public ClusterInfo couchbaseClusterInfo() throws Exception {
    return couchbaseCluster().clusterManager(getBucketName(), getBucketPassword()).info();
  }

  /**
   * Return the {@link Bucket} instance to connect to.
   *
   * @throws Exception on Bean construction failure.
   */
  @Override
  @Bean(destroyMethod = "close", name = BeanNames.COUCHBASE_BUCKET)
  public Bucket couchbaseClient() throws Exception {
    //@Bean method can use another @Bean method in the same @Configuration by directly invoking it
    return couchbaseCluster().openBucket(getBucketName(), getBucketPassword());
  }
}