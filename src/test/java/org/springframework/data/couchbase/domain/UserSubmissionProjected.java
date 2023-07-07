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

package org.springframework.data.couchbase.domain;

import java.util.List;

import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.couchbase.core.index.CompositeQueryIndex;
import org.springframework.data.couchbase.core.mapping.Document;

/**
 * UserSubmission entity for tests
 *
 * @author Michael Reiche
 */
@Document
@TypeAlias("user")
@CompositeQueryIndex(fields = { "id", "username", "email" })
public class UserSubmissionProjected extends ComparableEntity {
	private String id;
	private String username;
	private List<String> roles;
	private Address address;
	private List<Course> courses;

	public void setCourses(List<Course> courses) {
		this.courses = courses;
	}

    public String getUsername() {
        return username;
    }

    public String getId() {
        return id;
    }

    public List<Course> getCourses() {
        return courses;
    }

    public Address getAddress() {
        return address;
    }
}
