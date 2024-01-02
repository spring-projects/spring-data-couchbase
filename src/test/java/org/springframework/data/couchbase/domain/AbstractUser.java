/*
 * Copyright 2012-2024 the original author or authors
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

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.couchbase.core.mapping.Field;

/**
 * User entity for tests
 *
 * @author Michael Reiche
 */
@TypeAlias(AbstractingTypeMapper.Type.ABSTRACTUSER)
public abstract class AbstractUser extends ComparableEntity {
	@Id protected String id;
	protected String firstname;
	protected String lastname;
	@Field(AbstractingTypeMapper.SUBTYPE) protected String subtype;

	public String getId() {
		return id;
	}

	public String getFirstname() {
		return firstname;
	}
}
