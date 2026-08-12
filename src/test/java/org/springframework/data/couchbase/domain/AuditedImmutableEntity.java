/*
 * Copyright 2026 the original author or authors.
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

import java.time.Instant;

import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.annotation.Version;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.core.mapping.id.GeneratedValue;
import org.springframework.data.couchbase.core.mapping.id.GenerationStrategy;

/**
 * @author Artur Kalimullin
 */
@Document
public class AuditedImmutableEntity {

	@Id
	@GeneratedValue(strategy = GenerationStrategy.UNIQUE) private final String id;
	@Version private final long version;
	@CreatedDate private final Instant createdDate;
	@LastModifiedDate private final Instant lastModifiedDate;
	private final String value;

	public AuditedImmutableEntity(String id, long version, Instant createdDate, Instant lastModifiedDate, String value) {
		this.id = id;
		this.version = version;
		this.createdDate = createdDate;
		this.lastModifiedDate = lastModifiedDate;
		this.value = value;
	}

	public String getId() {
		return id;
	}

	public long getVersion() {
		return version;
	}

	public Instant getCreatedDate() {
		return createdDate;
	}

	public Instant getLastModifiedDate() {
		return lastModifiedDate;
	}
}
