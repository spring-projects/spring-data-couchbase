package org.springframework.data.couchbase.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.couchbase.core.index.CompositeQueryIndex;
import org.springframework.data.couchbase.core.index.QueryIndexed;
import org.springframework.data.couchbase.core.mapping.Document;

@Document
@CompositeQueryIndex(fields = {"id", "name desc"})
public class Airline {
	@Id
	String id;

	@QueryIndexed
	String name;

	@PersistenceConstructor
	public Airline(String id, String name) {
		this.id = id;
	}

	public String getId() {
		return id;
	}

	public String getName() {
		return name;
	}

}
