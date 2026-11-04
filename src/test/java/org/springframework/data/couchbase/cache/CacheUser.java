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
package org.springframework.data.couchbase.cache;

import java.io.Serializable;

/**
 * This is a standalone class (vs. inner) to allow Serialization of all fields to work.
 * If it was an inner class of CouchbaseCacheIntegrationTests, then it would have a
 * this$0 field = CouchbaseCacheIntegrationTests and would not serialize.
 *
 * @author Michael Reiche
 */
class CacheUser implements Serializable {
	// private static final long serialVersionUID = 8817717605659870262L;
	String firstname; // must have getter/setter for Serialize/Deserialize
	String lastname; // must have getter/setter for Serialize/Deserialize
	String id; // must have getter/setter for Serialize/Deserialize

	public CacheUser() {};

	public CacheUser(String id, String firstname, String lastname) {
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

	public void setId(String id) {
		this.id = id;
	}

	public void setFirstname(String firstname) {
		this.firstname = firstname;
	}

	public void setLastname(String lastname) {
		this.lastname = lastname;
	}
	// define equals for assertEquals()
	public boolean equals(Object o) {
		if (o == null) {
			return false;
		}
		if (!(o instanceof CacheUser)) {
			return false;
		}

		CacheUser other = (CacheUser) o;
		if (id == null && other.id != null) {
			return false;
		}
		if (firstname == null && other.firstname != null) {
			return false;
		}
		if (lastname == null && other.lastname != null) {
			return false;
		}
		return id.equals(other.id) && firstname.equals(other.firstname) && lastname.equals(other.lastname);
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("CacheUser: { id=" + id + ", firstname=" + firstname + ", lastname=" + lastname + "}");
		return sb.toString();
	}

}
