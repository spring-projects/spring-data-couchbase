/*
 * Copyright 2012-2020 the original author or authors
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

import java.util.Objects;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.couchbase.core.mapping.Document;

/**
 * User entity for tests
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */

@Document
public class UserJustLastName extends ComparableEntity {

	@Id private String id;
	private String lastname;

	public User user;

	@PersistenceConstructor
	public UserJustLastName(final String id, final String lastname) {
		this.id = id;
		this.lastname = lastname;
		this.user = new User("1", "first", "last");
	}

	public String getId() {
		return id;
	}

	public String getLastname() {
		return lastname;
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, lastname);
	}

}
