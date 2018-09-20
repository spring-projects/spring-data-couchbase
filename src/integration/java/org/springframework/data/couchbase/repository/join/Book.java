package org.springframework.data.couchbase.repository.join;

import org.springframework.data.annotation.Id;

/**
 * Book test class for N1QL Join tests
 */
public class Book {
	@Id
	String name;

	String authorName;

	String description;

	public Book(String name, String authorName, String description) {
		this.name = name;
		this.authorName = authorName;
		this.description = description;
	}

	public String getName() {
		return this.name;
	}

	public String getAuthorName() {
		return this.authorName;
	}

	public String getDescription() {
		return this.description;
	}
}