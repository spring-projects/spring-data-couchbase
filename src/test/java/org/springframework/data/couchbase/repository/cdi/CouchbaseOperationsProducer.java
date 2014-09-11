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

import javax.enterprise.inject.Produces;

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.CouchbaseTemplate;

import com.couchbase.client.CouchbaseClient;

/**
 * Produces a {@link CouchbaseOperations} instance for test usage.
 * @author Mark Paluch
 */
class CouchbaseOperationsProducer {

	@Produces
	public CouchbaseOperations createCouchbaseOperations(CouchbaseClient couchbaseClient) throws Exception {

		CouchbaseTemplate couchbaseTemplate = new CouchbaseTemplate(couchbaseClient);

		return couchbaseTemplate;
	}

}
