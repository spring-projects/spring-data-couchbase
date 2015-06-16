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

/**
 * Contains default bean names that will be used when no "id" is supplied to the beans.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 */
public class BeanNames {

	/**
	 * Refers to the <couchbase:env /> bean.
	 */
	static final String COUCHBASE_ENV = "couchbaseEnv";
	/**
	 * Refers to the "<couchbase:cluster />" bean.
	 */
	static final String COUCHBASE_CLUSTER = "couchbaseCluster";

	/**
	 * Refers to the "<couchbase:bucket />" bean.
	 */
	static final String COUCHBASE_BUCKET = "couchbaseBucket";

	/**
	 * Refers to the "<couchbase:template />" bean.
	 */
	static final String COUCHBASE_TEMPLATE = "couchbaseTemplate";

	/**
	 * Refers to the "<couchbase:translation-service />" bean
	 */
	static final String TRANSLATION_SERVICE = "translationService";


}
