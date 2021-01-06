/*
 * Copyright 2020 the original author or authors
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

/**
 * Submission entity for tests
 *
 * @author Michael Reiche
 */
public class Submission extends ComparableEntity {
	private final String id;
	private final String userId;
	private final String talkId;
	private final String status;
	private final long number;

	public Submission(String id, String userId, String talkId, String status, long number) {
		this.id = id;
		this.userId = userId;
		this.talkId = talkId;
		this.status = status;
		this.number = number;
	}

	public String getId() {
		return id;
	}

}
