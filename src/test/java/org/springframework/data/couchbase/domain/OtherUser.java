/*
 * Copyright 2022-present the original author or authors
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

import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.couchbase.core.mapping.Document;

/**
 * OtherUser entity for tests. Both User and OtherUser extend AbstractUser
 *
 * @author Michael Reiche
 */

@Document
@TypeAlias(AbstractingTypeMapper.Type.ABSTRACTUSER)
public class OtherUser extends AbstractUser {

	@PersistenceConstructor
	public OtherUser(final String id, final String firstname, final String lastname) {
		this.id = id;
		this.firstname = firstname;
		this.lastname = lastname;
		this.subtype = AbstractingTypeMapper.Type.OTHERUSER;
	}

}
