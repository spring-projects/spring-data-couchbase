/*
 * Copyright 2012-2021 the original author or authors
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

import org.springframework.data.annotation.CreatedBy;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedBy;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.annotation.Transient;
import org.springframework.data.annotation.Version;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.core.mapping.Field;
import org.springframework.data.couchbase.repository.TransactionResult;
import org.springframework.data.domain.Persistable;
import org.springframework.lang.Nullable;

import java.util.Optional;
import java.util.UUID;

// todo gpx: lame to C&P the entire Person, but struggling to get a simpler entity working
@Document
public class PersonWithoutVersion extends AbstractEntity implements Persistable<Object> {
	Optional<String> firstname;
	@Nullable Optional<String> lastname;

	@CreatedBy private String creator;

	@LastModifiedBy private String lastModifiedBy;

	@LastModifiedDate private long lastModification;

	@CreatedDate private long creationDate;

	@Nullable @Field("nickname") private String middlename;
	@Nullable @Field(name = "prefix") private String salutation;

	private Address address;

	// Required for use in transactions
	@TransactionResult private Integer txResultHolder;
	@Transient private boolean isNew;


	public PersonWithoutVersion() {}

	public PersonWithoutVersion(String firstname, String lastname) {
		this();
		setFirstname(firstname);
		setLastname(lastname);
		setMiddlename("Nick");
		isNew(true);
	}

	public PersonWithoutVersion(int id, String firstname, String lastname) {
		this(firstname, lastname);
		setId(new UUID(id, id));
	}

	public PersonWithoutVersion(UUID id, String firstname, String lastname) {
		this(firstname, lastname);
		setId(id);
	}

	static String optional(String name, Optional<String> obj) {
		if (obj != null) {
			if (obj.isPresent()) {
				return ("  " + name + ": '" + obj.get() + "'");
			} else {
				return "  " + name + ": null";
			}
		}
		return "";
	}

	public String getFirstname() {
		return firstname.get();
	}

	public void setFirstname(String firstname) {
		this.firstname = firstname == null ? null : (Optional.ofNullable(firstname.equals("") ? null : firstname));
	}

	public void setFirstname(Optional<String> firstname) {
		this.firstname = firstname;
	}

	public String getLastname() {
		return lastname.get();
	}

	public void setLastname(String lastname) {
		this.lastname = lastname == null ? null : (Optional.ofNullable(lastname.equals("") ? null : lastname));
	}

	public void setLastname(Optional lastname) {
		this.lastname = lastname;
	}

	public String getMiddlename() {
		return middlename;
	}

	public String getSalutation() {
		return salutation;
	}

	public void setMiddlename(String middlename) {
		this.middlename = middlename;
	}

	public void setSalutation(String salutation) {
		this.salutation = salutation;
	}

	public Address getAddress() {
		return address;
	}

	public void setAddress(Address address) {
		this.address = address;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Person : {\n");
		sb.append("  id : " + getId());
		sb.append(optional(", firstname", firstname));
		sb.append(optional(", lastname", lastname));
		if (middlename != null)
			sb.append(", middlename : '" + middlename + "'");
		if (creator != null) {
			sb.append(", creator : " + creator);
		}
		if (creationDate != 0) {
			sb.append(", creationDate : " + creationDate);
		}
		if (lastModifiedBy != null) {
			sb.append(", lastModifiedBy : " + lastModifiedBy);
		}
		if (lastModification != 0) {
			sb.append(", lastModification : " + lastModification);
		}
		if (getAddress() != null) {
			sb.append(", address : " + getAddress().toString());
		}
		sb.append("\n}");
		return sb.toString();
	}

	public PersonWithoutVersion withFirstName(String firstName) {
		PersonWithoutVersion p = new PersonWithoutVersion(this.getId(), firstName, this.getLastname());
		p.txResultHolder = this.txResultHolder;
		return p;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!super.equals(obj)) {
			return false;
		}

		PersonWithoutVersion that = (PersonWithoutVersion) obj;
		return this.getId().equals(that.getId()) && this.getFirstname().equals(that.getFirstname())
				&& this.getLastname().equals(that.getLastname()) && this.getMiddlename().equals(that.getMiddlename());
	}

	@Override
	public boolean isNew() {
		return isNew;
	}

	public void isNew(boolean isNew){
		this.isNew = isNew;
	}
}