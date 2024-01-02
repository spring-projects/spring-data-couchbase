/*
 * Copyright 2018-2024 the original author or authors
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

package org.springframework.data.couchbase.core.query;

/**
 * Hash side to specify hash join. Here based on probe or build, the entity will be used to query or build the hash
 * table. The smaller data set side should be used to build to fit in memory.
 *
 * @author Subhashni Balakrishnan
 */
public enum HashSide {
	/**
	 * Hash join will not be used
	 */
	NONE("none"),

	/**
	 * Associated entity will be on the probe side of the hash table
	 */
	PROBE("probe"),

	/**
	 * Associated entity will be used to build the hash table for faster lookup
	 */
	BUILD("build");

	private final String value;

	HashSide(String value) {
		this.value = value;
	}

	public String getValue() {
		return this.value;
	}
}
