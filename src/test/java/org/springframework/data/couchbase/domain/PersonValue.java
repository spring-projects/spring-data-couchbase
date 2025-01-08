/*
 * Copyright 2020-2025 the original author or authors
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
import org.springframework.data.annotation.Version;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.core.mapping.Field;
import org.springframework.data.couchbase.core.mapping.id.GeneratedValue;
import org.springframework.data.couchbase.core.mapping.id.GenerationStrategy;

/**
 * PersonValue entity for tests
 *
 * @author Michael Reiche
 */

@Document
public class PersonValue {
	@Id @GeneratedValue(strategy = GenerationStrategy.UNIQUE)
    private final String id;
    @Version private final long version;
    @Field private final String firstname;
    @Field private final String lastname;

	public PersonValue(String id, long version, String firstname, String lastname) {
		this.id = id;
		this.version = version;
		this.firstname = firstname;
		this.lastname = lastname;
	}

    public PersonValue withId(String id) {
        return new PersonValue(id, this.version, this.firstname, this.lastname);
    }

    public PersonValue withVersion(Long version) {
        return new PersonValue(this.id, version, this.firstname, this.lastname);
    }

    public PersonValue withFirstname(String firstname) {
        return new PersonValue(this.id, this.version, firstname, this.lastname);
    }

    public PersonValue withLastname(String lastname) {
        return new PersonValue(this.id, this.version, this.firstname, lastname);
    }

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("PersonValue : {");
        sb.append(" id : " + id);
		sb.append(", version : " + version);
		sb.append(", firstname : " + firstname);
		sb.append(", lastname : " + lastname);
		sb.append(" }");
		return sb.toString();
	}

    public String getId() {
        return id;
    }

    public long getVersion() {
        return version;
    }

    public boolean equals(Object other) {
        if (other == null || !(other instanceof PersonValue)) {
            return false;
        }
        PersonValue that = (PersonValue) other;
        return equals(this.getId(), that.getId()) && equals(this.version, that.version)
                && equals(this.firstname, that.firstname) && equals(this.lastname, that.lastname);
    }

    boolean equals(Object s0, Object s1) {
        if (s0 == null && s1 == null || s0 == s1) {
            return true;
        }
        Object sa = s0 != null ? s0 : s1;
        Object sb = s0 != null ? s1 : s0;
        return sa.equals(sb);
    }
}
