/*
 * Copyright 2022-2025 the original author or authors
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

import org.springframework.data.couchbase.core.convert.CouchbaseCustomConversions;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.mapping.context.MappingContext;

/**
 * MappingConverter that uses AbstractTypeMapper
 *
 * @author Michael Reiche
 */
public class AbstractingMappingCouchbaseConverter extends MappingCouchbaseConverter {

	/**
	 * this constructer creates a TypeBasedCouchbaseTypeMapper with the specified typeKey while MappingCouchbaseConverter
	 * uses a DefaultCouchbaseTypeMapper typeMapper = new DefaultCouchbaseTypeMapper(typeKey != null ? typeKey :
	 * TYPEKEY_DEFAULT);
	 *
	 * @param mappingContext
	 * @param typeKey - the typeKey to be used (normally "_class")
	 */
	public AbstractingMappingCouchbaseConverter(
			final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext,
			final String typeKey,
			final CouchbaseCustomConversions couchbaseCustomConversions) {
		super(mappingContext, typeKey, couchbaseCustomConversions);
		this.typeMapper = new AbstractingTypeMapper(typeKey);
	}

}
