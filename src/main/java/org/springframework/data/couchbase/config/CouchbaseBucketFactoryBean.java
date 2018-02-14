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

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseBucket;

import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.couchbase.core.CouchbaseExceptionTranslator;

/**
 * The Factory Bean to help {@link CouchbaseBucketParser} constructing a {@link Bucket} from a given
 * {@link Cluster} reference.
 *
 * @author Simon Baslé
 * @author Subhashni Balakrishnan
 */
public class CouchbaseBucketFactoryBean extends AbstractFactoryBean<Bucket> implements PersistenceExceptionTranslator {

	private final Cluster cluster;
	private final String bucketName;
	private final String username;
	private final String password;

	private final PersistenceExceptionTranslator exceptionTranslator = new CouchbaseExceptionTranslator();

	public CouchbaseBucketFactoryBean(Cluster cluster) {
		this(cluster, null, null, null);
	}

	public CouchbaseBucketFactoryBean(Cluster cluster, String bucketName) {
		this(cluster, bucketName, bucketName, null);
	}

	public CouchbaseBucketFactoryBean(Cluster cluster, String bucketName, String username, String password) {
		this.cluster = cluster;
		this.bucketName = bucketName;
		this.username = username;
		this.password = password;
	}

	@Override
	public Class<?> getObjectType() {
		return CouchbaseBucket.class;
	}

	@Override
	protected Bucket createInstance() throws Exception {
		if (bucketName == null) {
			return cluster.openBucket();
		}
		else if (password == null) {
			return cluster.openBucket(bucketName);
		}
		else if (bucketName.contentEquals(username)) {
			return cluster.openBucket(bucketName, password);
		}
		else {
			cluster.authenticate(username, password);
			return cluster.openBucket(bucketName);
		}
	}

	@Override
	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		return exceptionTranslator.translateExceptionIfPossible(ex);
	}
}
