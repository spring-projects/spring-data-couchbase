/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.repository.cdi;

import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;

import org.springframework.data.couchbase.config.CouchbaseBucketFactoryBean;

/**
 * Producer for {@link Bucket}. A default {@link CouchbaseCluster} with defaults
 * from {@link CouchbaseBucketFactoryBean} are sufficient for our test.
 * 
 * @author Mark Paluch
 * @author Simon Baslé
 */
class CouchbaseClientProducer {

	@Produces
	public Bucket createCouchbaseClient() throws Exception {
		//FIXME produce a Cluster and close it properly?
		CouchbaseBucketFactoryBean couchbaseFactoryBean = new CouchbaseBucketFactoryBean(
				CouchbaseCluster.create(), "default");
		couchbaseFactoryBean.afterPropertiesSet();
		return couchbaseFactoryBean.getObject();
	}

	public void close(@Disposes Bucket couchbaseClient) {
		couchbaseClient.close();
	}
}
