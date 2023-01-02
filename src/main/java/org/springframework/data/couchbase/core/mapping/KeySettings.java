/*
 * Copyright 2017-2023 the original author or authors
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

package org.springframework.data.couchbase.core.mapping;

/**
 * Common settings for Couchbase key - prefix - suffix - delimiter
 *
 * @author Subhashni Balakrishnan
 */
public class KeySettings {

	private static String DEFAULT_DELIMITER = ".";

	private String commonPrefix;

	private String commonSuffix;

	private String delimiter;

	protected KeySettings() {
		this.delimiter = DEFAULT_DELIMITER;
	}

	public static KeySettings build() {
		return new KeySettings();
	}

	/**
	 * Set common prefix
	 */
	public KeySettings prefix(String prefix) {
		this.commonPrefix = prefix;
		return this;
	}

	/**
	 * Set common suffix
	 */
	public KeySettings suffix(String suffix) {
		this.commonSuffix = suffix;
		return this;
	}

	/**
	 * Set delimiter
	 */
	public KeySettings delimiter(String delimiter) {
		if (delimiter == null) {
			return this;
		}
		this.delimiter = delimiter;
		return this;
	}

	/**
	 * Get common prefix
	 */
	public String prefix() {
		return this.commonPrefix;
	}

	/**
	 * Get common suffix
	 */
	public String suffix() {
		return this.commonSuffix;
	}

	/**
	 * Get delimiter
	 */
	public String delimiter() {
		return this.delimiter;
	}
}
