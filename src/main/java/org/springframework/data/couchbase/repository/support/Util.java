/*
 * Copyright 2020-2023 the original author or authors
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

package org.springframework.data.couchbase.repository.support;

import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;

/**
 * Utility class for Couchbase Repository
 *
 * @author Michael Reiche
 */
public class Util {

	public static boolean hasNonZeroVersionProperty(Object entity, CouchbaseConverter converter) {
		CouchbasePersistentEntity<?> mapperEntity = converter.getMappingContext().getPersistentEntity(entity.getClass());
		final CouchbasePersistentProperty versionProperty = mapperEntity.getVersionProperty();
		boolean hasVersionProperty = false;
		try {
			if (versionProperty != null && versionProperty.getField() != null) {
				Object versionValue = versionProperty.getField().get(entity);
				hasVersionProperty = versionValue != null && !versionValue.equals(Long.valueOf(0));
			}
		} catch (IllegalAccessException iae) {}
		return hasVersionProperty;
	}
}
