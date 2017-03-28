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

package org.springframework.data.couchbase.core;

import com.couchbase.client.java.repository.annotation.Field;
import lombok.EqualsAndHashCode;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;


/**
 * Test class for persisting and loading from {@link RxJavaCouchbaseTemplate}.
 *
 * @author Alex Derkach
 */
@EqualsAndHashCode
public class VersionedReactiveBeer {

	@Id
	private final String id;

	private String name;

	@Field("is_active")
	private boolean active = true;

	@Field("desc")
	private String description;

	@Version
	private long version;

	public VersionedReactiveBeer(String id, String name, Boolean active, String description) {
		this.id = id;
		this.name = name;
		this.active = active;
		this.description = description;
	}

	@Override
	public String toString() {
		return "Beer [id=" + id + ", name=" + name + ", active=" + active + ", description=" + description + "]";
	}

	public VersionedReactiveBeer setName(String name) {
		this.name = name;
		return this;
	}

	public String getName() {
		return name;
	}

	public VersionedReactiveBeer setActive(boolean active) {
		this.active = active;
		return this;
	}

	public boolean getActive() {
		return active;
	}

	public VersionedReactiveBeer setDescription(String description) {
		this.description = description;
		return this;
	}

	public String getDescription() {
		return description;
	}

	public String getId() {
		return id;
	}

	public void setVersion(long version) {
		this.version = version;
	}

	public long getVersion() {
		return version;
	}
}
