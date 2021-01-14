/*
 * Copyright 2020 the original author or authors
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

package org.springframework.data.couchbase.domain;

import com.couchbase.mock.deps.com.google.gson.Gson;
import com.couchbase.mock.deps.com.google.gson.GsonBuilder;

/**
 * Comparable entity base class for tests
 *
 * @author Michael Reiche
 */
public class ComparableEntity {

	/**
	 * equals() method that relies on toString()
	 * 
	 * @param that
	 * @return
	 * @throws RuntimeException
	 */
	@Override
	public boolean equals(Object that) throws RuntimeException {
		if (this == that) {
			return true;
		}
		if (that == null
				|| !(this.getClass().isAssignableFrom(that.getClass()) || that.getClass().isAssignableFrom(this.getClass()))) {
			return false;
		}
		return this.toString().equals(that.toString());

	}

	public String toString() throws RuntimeException {
		Gson gson = new GsonBuilder().create();
		return gson.toJson(this);
	}
}
