package org.springframework.data.couchbase.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.core.mapping.Field;

import java.util.List;
import java.util.Map;

@Document
public class MyClass {

	public MyClass() {}

	public void setId(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}

	public List<Map<String, Object>> getFields() {
		return fields;
	}

	public void setFields(List<Map<String, Object>> fields) {
		this.fields = fields;
	}

	public List<Map<String, Object>> getConfiguration() {
		return configuration;
	}

	public void setConfiguration(List<Map<String, Object>> configuration) {
		this.configuration = configuration;
	}

	@Id private String id;
	@Field public List<Map<String, Object>> fields;
	@Field public List<Map<String, Object>> configuration;

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("{ fields : ");
		sb.append(getFields().toString());
		sb.append(" }");
		return sb.toString();
	}
}
