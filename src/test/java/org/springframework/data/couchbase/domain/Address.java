package org.springframework.data.couchbase.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.core.mapping.id.GeneratedValue;
import org.springframework.data.couchbase.core.mapping.id.GenerationStrategy;

import java.util.UUID;

@Document
public class Address extends AbstractEntity {

	private	String street;

	public Address() {}

	public String getStreet() {
		return street;
	}

	public void setStreet(String street) {
		this.street = street;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("{\"street\"=\"");
		sb.append(getStreet());
		sb.append("\"}");
		return sb.toString();
	}
}
