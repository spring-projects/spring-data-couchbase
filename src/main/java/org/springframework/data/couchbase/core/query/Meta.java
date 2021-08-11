/*
 * Copyright 2020 the original author or authors.
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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Meta-data for {@link Query} instances.
 *
 * @author Michael Reiche
 */
public class Meta {

	private enum MetaKey {
		EXAMPLE("$example");

		private String key;

		MetaKey(String key) {
			this.key = key;
		}
	}

	private final Map<String, Object> values = new LinkedHashMap<>(2);

	public Meta() {}

	/**
	 * Copy a {@link Meta} object.
	 *
	 * @since 2.2
	 * @param source
	 */
	Meta(Meta source) {
		this.values.putAll(source.values);
	}

	/**
	 * @return
	 */
	public boolean hasValues() {
		return !this.values.isEmpty();
	}

	/**
	 * Get {@link Iterable} of set meta values.
	 *
	 * @return
	 */
	public Iterable<Entry<String, Object>> values() {
		return Collections.unmodifiableSet(this.values.entrySet());
	}

	/**
	 * Sets or removes the value in case of {@literal null} or empty {@link String}.
	 *
	 * @param key must not be {@literal null} or empty.
	 * @param value
	 */
	void setValue(String key, @Nullable Object value) {

		Assert.hasText(key, "Meta key must not be 'null' or blank.");

		if (value == null || (value instanceof String && !StringUtils.hasText((String) value))) {
			this.values.remove(key);
		}
		this.values.put(key, value);
	}

	@Nullable
	@SuppressWarnings("unchecked")
	private <T> T getValue(String key) {
		return (T) this.values.get(key);
	}

	private <T> T getValue(String key, T defaultValue) {

		T value = getValue(key);
		return value != null ? value : defaultValue;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {

		int hash = ObjectUtils.nullSafeHashCode(this.values);
		return hash;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {

		if (this == obj) {
			return true;
		}

		if (!(obj instanceof Meta)) {
			return false;
		}

		Meta other = (Meta) obj;
		return ObjectUtils.nullSafeEquals(this.values, other.values);
	}

}
