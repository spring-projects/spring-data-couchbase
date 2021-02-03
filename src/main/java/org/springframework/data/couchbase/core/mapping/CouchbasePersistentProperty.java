/*
 * Copyright 2012-2021 the original author or authors
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

import org.springframework.data.mapping.PersistentProperty;

/**
 * Represents a property part of an entity that needs to be persisted.
 *
 * @author Michael Nitschinger
 */
public interface CouchbasePersistentProperty extends PersistentProperty<CouchbasePersistentProperty> {

	/**
	 * Returns the field name of the property.
	 * <p/>
	 * The field name can be different from the actual property name by using a custom annotation.
	 */
	String getFieldName();

	Boolean isExpirationProperty();
}
