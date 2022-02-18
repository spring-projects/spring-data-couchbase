/*
 * Copyright 2012-2022 the original author or authors
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

package org.springframework.data.couchbase.core.convert;

import org.springframework.data.convert.EntityConverter;
import org.springframework.data.convert.EntityReader;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.mapping.Alias;
import org.springframework.data.util.TypeInformation;

/**
 * Marker interface for the converter, identifying the types to and from that can be converted.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 * @author Michael Reiche
 */
public interface CouchbaseConverter
		extends EntityConverter<CouchbasePersistentEntity<?>, CouchbasePersistentProperty, Object, CouchbaseDocument>,
		CouchbaseWriter<Object, CouchbaseDocument>, EntityReader<Object, CouchbaseDocument> {

	/**
	 * Convert the value if necessary to the class that would actually be stored, or leave it as is if no conversion
	 * needed.
	 *
	 * @param value the value to be converted to the class that would actually be stored.
	 * @return the converted value (or the same value if no conversion necessary).
	 */
	Object convertForWriteIfNeeded(Object value);

	/**
	 * Return the Class that would actually be stored for a given Class.
	 *
	 * @param clazz the source class.
	 * @return the target class that would actually be stored.
	 * @see #convertForWriteIfNeeded(Object)
	 */
	Class<?> getWriteClassFor(Class<?> clazz);

	/**
	 * @return the name of the field that will hold type information.
	 */
	String getTypeKey();

	/**
	 * @return the alias value for the type
	 */
	Alias getTypeAlias(TypeInformation<?> info);

	// TODO needed later
	// CouchbaseTypeMapper getMapper();
	// Object convertToCouchbaseType(Object source, TypeInformation<?> typeInformation);
	//
	// Object convertToCouchbaseType(String source);
}
