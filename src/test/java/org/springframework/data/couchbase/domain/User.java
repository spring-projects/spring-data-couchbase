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

import org.springframework.data.annotation.CreatedBy;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedBy;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.annotation.Version;
import org.springframework.data.couchbase.core.mapping.Document;

/**
 * User entity for tests
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */

@Document
public class User extends ComparableEntity {

	@Version long version;
	@Id private String id;
	private String firstname;
	private String lastname;
	@CreatedBy private String createdBy;
	@CreatedDate private long createdDate;
	@LastModifiedBy private String lastModifiedBy;
	@LastModifiedDate private long lastModifiedDate;

	@PersistenceConstructor
	public User(final String id, final String firstname, final String lastname) {
		this.id = id;
		this.firstname = firstname;
		this.lastname = lastname;
	}

	public String getId() {
		return id;
	}

	public String getFirstname() {
		return firstname;
	}

	public String getLastname() {
		return lastname;
	}

	public long getCreatedDate() {
		return createdDate;
	}

	public String getCreator() {
		return createdBy;
	}

	public long getLastModifiedDate() {
		return lastModifiedDate;
	}

	public String getLastModifiedBy() {
		return lastModifiedBy;
	}

	public long getVersion() {
		return version;
	}

	public void setVersion(long version) {
		this.version = version;
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, firstname, lastname);
	}

}
