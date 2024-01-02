/*
 * Copyright 2020-2024 the original author or authors
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

import lombok.Data;

import java.util.List;

import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.couchbase.core.index.CompositeQueryIndex;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.core.query.FetchType;
import org.springframework.data.couchbase.core.query.N1qlJoin;

/**
 * UserSubmission entity for tests
 *
 * @author Michael Reiche
 */
@Data
@Document
@TypeAlias("user")
@CompositeQueryIndex(fields = { "id", "username", "email" })
public class UserSubmission extends ComparableEntity {
	private String id;
	private String username;
	private String email;
	private String password;
	private List<String> roles;
	private Address address;
	@N1qlJoin(on = "meta(lks).id=rks.parentId", fetchType = FetchType.IMMEDIATE) List<Address> otherAddresses;

	private int credits;
	private List<Submission> submissions;
	private List<Course> courses;

	public void setSubmissions(List<Submission> submissions) {
		this.submissions = submissions;
	}

	public void setCourses(List<Course> courses) {
		this.courses = courses;
	}

}
