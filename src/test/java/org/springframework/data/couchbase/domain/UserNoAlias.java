/*
 * Copyright 2012-2025 the original author or authors
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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.springframework.data.annotation.CreatedBy;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedBy;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.annotation.PersistenceCreator;
import org.springframework.data.annotation.Transient;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.annotation.Version;
import org.springframework.data.couchbase.core.mapping.Document;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;

/**
 * User entity with an empty TypeAlias for tests
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */

@Document
@TypeAlias("")
public class UserNoAlias extends AbstractUser implements Serializable {

	public JsonNode jsonNode;
	public JsonObject jsonObject;
	public JsonArray jsonArray;

	@PersistenceCreator
	public UserNoAlias(final String id, final String firstname, final String lastname) {
		this.id = id;
		this.firstname = firstname;
		this.lastname = lastname;
		this.subtype = AbstractingTypeMapper.Type.USER;
		this.jsonNode = new ObjectNode(JsonNodeFactory.instance);
		try {
			jsonNode = (new ObjectNode(JsonNodeFactory.instance)).put("myNumber", uid());
		} catch (Exception e) {
			e.printStackTrace();
		}
		Map map = new HashMap();
		map.put("myNumber", uid());
		this.jsonObject = JsonObject.jo().put("yourNumber",Long.valueOf(uid()));
		this.jsonArray = JsonArray.from(Long.valueOf(uid()), Long.valueOf(uid()));
	}

	@Transient int uid=1000;
	long uid(){
		return uid++;
	}


	@Version protected long version;
	@Transient protected String transientInfo;
	@CreatedBy protected String createdBy;
	@CreatedDate protected long createdDate;
	@LastModifiedBy protected String lastModifiedBy;
	@LastModifiedDate protected long lastModifiedDate;

	public String getLastname() {
		return lastname;
	}

	public long getCreatedDate() {
		return createdDate;
	}

	public void setCreatedDate(long createdDate) {
		this.createdDate = createdDate;
	}

	public String getCreatedBy() {
		return createdBy;
	}

	public void setCreatedBy(String createdBy) {
		this.createdBy = createdBy;
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
		return Objects.hash(getId(), firstname, lastname);
	}

	public String getTransientInfo() {
		return transientInfo;
	}

	public void setTransientInfo(String something) {
		transientInfo = something;
	}

}
