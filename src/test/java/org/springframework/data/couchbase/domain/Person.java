<<<<<<< HEAD
package org.springframework.data.couchbase.domain;

//import static com.mysema.query.collections.MiniApi.*;
//import com.mysema.query.annotations.QueryEntity;

import org.springframework.data.annotation.*;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.core.mapping.event.AuditingEventListener;
//import org.springframework.data.couchbase.repository.auditing.EnableCouchbaseAuditing;

import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;

//@QueryEntity
@Document
//@EntityListeners(AuditingEventListener.class)
public class Person extends AbstractEntity {
	String firstname;
	String lastname;

	@CreatedBy
	private String creator;

	@LastModifiedBy
	private String lastModifiedBy;

	@LastModifiedDate
	private long lastModification;

	@CreatedDate
	private long creationDate;

	@Version
	private long version;

	public Person() {
	}
=======
/*
 * Copyright 2012-2020 the original author or authors
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

import java.util.Optional;
import java.util.UUID;

import org.springframework.data.annotation.CreatedBy;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedBy;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.annotation.Version;
import org.springframework.data.couchbase.core.mapping.Document;

@Document
public class Person extends AbstractEntity {
	Optional<String> firstname;
	Optional<String> lastname;

	@CreatedBy private String creator;

	@LastModifiedBy private String lastModifiedBy;

	@LastModifiedDate private long lastModification;

	@CreatedDate private long creationDate; // =System.currentTimeMillis();

	@Version private long version;

	public Person() {}
>>>>>>> 7773a19e4230817b06506fba171265edc512fee1

	public Person(String firstname, String lastname) {
		this();
		setFirstname(firstname);
		setLastname(lastname);
	}

	public Person(int id, String firstname, String lastname) {
		this(firstname, lastname);
		setId(new UUID(id, id));
	}

<<<<<<< HEAD
	public String getFirstname() {
=======
	static String optional(String name, Optional<String> obj) {
		if (obj != null)
			if (obj.isPresent())
				return ("  " + name + ": '" + obj.get() + "'\n");
			else
				return "  " + name + ": null\n";
		return "";
	}

	public Optional<String> getFirstname() {
>>>>>>> 7773a19e4230817b06506fba171265edc512fee1
		return firstname;
	}

	public void setFirstname(String firstname) {
<<<<<<< HEAD
		this.firstname = firstname; // == null ? null : (Optional.ofNullable(firstname.equals("") ? null : firstname));
	}

	//public void setFirstname(Optional<String> firstname) {
	//	this.firstname = firstname;
	//}

	public String getLastname() {
=======
		this.firstname = firstname == null ? null : (Optional.ofNullable(firstname.equals("") ? null : firstname));
	}

	public void setFirstname(Optional<String> firstname) {
		this.firstname = firstname;
	}

	public Optional<String> getLastname() {
>>>>>>> 7773a19e4230817b06506fba171265edc512fee1
		return lastname;
	}

	public void setLastname(String lastname) {
<<<<<<< HEAD
		this.lastname = lastname ; // == null ? null : (Optional.ofNullable(lastname.equals("") ? null : lastname));
	}

	// public void setLastname(Optional lastname) {
	//	this.lastname = lastname;
	//}
=======
		this.lastname = lastname == null ? null : (Optional.ofNullable(lastname.equals("") ? null : lastname));
	}

	public void setLastname(Optional lastname) {
		this.lastname = lastname;
	}
>>>>>>> 7773a19e4230817b06506fba171265edc512fee1

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Person : {\n");
		sb.append("  id : " + getId());
		sb.append(optional(", firstname", firstname));
		sb.append(optional(", lastname", lastname));
		sb.append(", version : " + version);
		if (creator != null)
			sb.append(", creator : " + creator);
		if (creationDate != 0)
			sb.append(", creationDate : " + creationDate);
		if (lastModifiedBy != null)
			sb.append(", lastModifiedBy : " + lastModifiedBy);
		if (lastModification != 0)
			sb.append(", lastModification : " + lastModification);
		sb.append("}");
		return sb.toString();
	}
<<<<<<< HEAD

	static String optional(String name, String obj) {
		if (obj != null)
			if (obj != null /*.isPresent() */)
				return ("  " + name + ": '" + obj/*.get()*/ + "'\n");
			else
				return "  " + name + ": null\n";
		return "";
	}
=======
>>>>>>> 7773a19e4230817b06506fba171265edc512fee1
}
