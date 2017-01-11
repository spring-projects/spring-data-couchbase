/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.repository;

import org.springframework.data.annotation.Id;

/**
 * @author Subhashni Balakrishnan
 */
public class ReactiveUser {

	@Id
	private final String key;

	private final String username;

	private final int age;

	public ReactiveUser(String key, String username, int age) {
		this.key = key;
		this.username = username;
		this.age = age;
	}

	public String getUsername() {
		return username;
	}

	public String getKey() {
		return key;
	}

	public int getAge() {
		return age;
	}

	@Override
	public String toString() {
		return this.key;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		ReactiveUser user = (ReactiveUser) o;
		return key.equals(user.key);
	}

	@Override
	public int hashCode() {
		return key.hashCode();
	}
}
