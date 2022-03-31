/*
 * Copyright 2010-2022 the original author or authors.
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
package org.springframework.data.couchbase.core.query;

import org.springframework.data.couchbase.core.convert.CouchbaseConverter;

import com.couchbase.client.java.json.JsonValue;

/**
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Michael Reiche
 */
public interface QueryCriteriaDefinition {

	/**
	 * This exports the query criteria into a string to be appended to the beginning of an N1QL statement
	 *
	 * @param paramIndexPtr - this is a reference to the parameter index to be used for positional parameters There may
	 *          already be positional parameters in the beginning of the statement, so it may not always start at 1. If it
	 *          has the value -1, the query is using named parameters. If the pointer is null, the query is not using
	 *          parameters.
	 * @param parameters - query parameters. Criteria values that are converted to arguments are added to parameters
	 * @param converter - converter to use for converting criteria values
	 * @return string containing part of N1QL query
	 */
	String export(int[] paramIndexPtr, JsonValue parameters, CouchbaseConverter converter);

	/**
	 * Export the query criteria to a string without using positional or named parameters.
	 *
	 * @return string containing part of N1QL query
	 */
	String export();

	void setChainOperator(QueryCriteria.ChainOperator chainOperator);
}
