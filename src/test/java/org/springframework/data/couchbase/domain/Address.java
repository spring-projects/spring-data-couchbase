package org.springframework.data.couchbase.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.core.mapping.id.GeneratedValue;
import org.springframework.data.couchbase.core.mapping.id.GenerationStrategy;

import java.util.UUID;

@Document
public class Address extends AbstractEntity {

	// from AbstractEntity @Id @GeneratedValue(strategy = GenerationStrategy.UNIQUE) private UUID id;

	String street; // until MappingCouchbaseConverter.readFromMap handles setter/getter and/or Constructor

	public Address(){	}
	public String getStreet(){
		return street;
	}
	public void setStreet(String street){
		this.street = street;
	}

	/* from AbstractEntity
	public UUID getId(){
		return id;
	}

	public void setId(UUID id){
		this.id = id;
	}
	 */

	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("{ street : ");
		sb.append( getStreet() );
		sb.append(" }");
		return sb.toString();
	}
}
