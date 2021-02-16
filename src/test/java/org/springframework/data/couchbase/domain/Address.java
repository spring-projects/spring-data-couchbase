package org.springframework.data.couchbase.domain;

import org.springframework.data.couchbase.core.mapping.Document;

@Document
public class Address extends ComparableEntity {

	private String street;
	private String city;

	public Address() {}

	public String getStreet() {
		return street;
	}

	public void setStreet(String street) {
		this.street = street;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

}
