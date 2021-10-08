/*
 * Copyright 2012-2021 the original author or authors
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

/**
 * Contains default bean names for Couchbase beans. These are the names of the beans used by Spring Data Couchbase,
 * unless an explicit id is given to the bean either in the xml configuration or the
 * {@link AbstractCouchbaseConfiguration java configuration}.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 * @author Michael Reiche
 * @author Jorge Rodríguez Martín
 */
public class BeanNames {

	public static final String COUCHBASE_TEMPLATE = "couchbaseTemplate";

	public static final String REACTIVE_COUCHBASE_TEMPLATE = "reactiveCouchbaseTemplate";

	public static final String COUCHBASE_CUSTOM_CONVERSIONS = "couchbaseCustomConversions";

	public static final String COUCHBASE_TRANSACTIONS = "couchbaseTransactions";

	/**
	 * The name for the bean that stores custom mapping between repositories and their backing couchbaseOperations.
	 */
	public static final String COUCHBASE_OPERATIONS_MAPPING = "couchbaseRepositoryOperationsMapping";

	/**
	 * The name for the bean that stores custom mapping between reactive repositories and their backing
	 * reactiveCouchbaseOperations.
	 */
	public static final String REACTIVE_COUCHBASE_OPERATIONS_MAPPING = "reactiveCouchbaseRepositoryOperationsMapping";

	/**
	 * The name for the bean that stores mapping metadata for entities stored in couchbase.
	 */
	public static final String COUCHBASE_MAPPING_CONTEXT = "couchbaseMappingContext";

	/**
	 * The name for the bean that will handle audit trail marking of entities.
	 */
	public static final String COUCHBASE_AUDITING_HANDLER = "couchbaseAuditingHandler";

	/**
	 * The name for the bean that will handle reactive audit trail marking of entities.
	 */
	public static final String REACTIVE_COUCHBASE_AUDITING_HANDLER = "reactiveCouchbaseAuditingHandler";

	public static final String COUCHBASE_CLIENT_FACTORY = "couchbaseClientFactory";

}
