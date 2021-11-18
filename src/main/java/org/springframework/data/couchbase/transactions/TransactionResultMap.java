/*
 * Copyright 2021 the original author or authors
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
package org.springframework.data.couchbase.transactions;

import java.util.HashMap;
import java.util.Map;

import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.transaction.TransactionUsageException;

import com.couchbase.transactions.TransactionGetResult;

/**
 * Map of TransactionGetResult
 *
 * @author Michael Reiche
 */
public class TransactionResultMap extends HashMap<Object, TransactionGetResult>
		implements Map<Object, TransactionGetResult> {

	CouchbaseTemplate template;

	public TransactionResultMap(CouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public TransactionGetResult get(Object o) {
		Integer key = System.identityHashCode(o);
		TransactionGetResult result = super.get(key);
		if (result == null) { // not a previously read object, but maybe a copy of one with that should hold the key
			key = template.support().getTxResultKey(o);
			result = super.get(key);
			if (result == null) {
				throw new TransactionUsageException(
						"could not find the TransactionGetResult in the TransactionResultMap despite having a key. object = " + o);
			}
		}
		return result;
	}

	/**
	 * called by decodeEntity and applyResult to save the transctionGetResult of the object
	 */

	public Integer save(Object o, TransactionGetResult result) {
		// always use identifyHashCode even if there is a template.support().getTxResultKey(o)
		Integer key = System.identityHashCode(o);
		put(key, result);
		return key; // used by template.support decodeEntity() and applyResult() to save in entity
	}

}
