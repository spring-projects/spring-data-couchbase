package org.springframework.data.couchbase.repository.join;
import java.util.List;

import com.couchbase.client.java.repository.annotation.Field;
import org.springframework.data.annotation.Id;
import org.springframework.data.couchbase.core.query.FetchType;
import org.springframework.data.couchbase.core.query.N1qlJoin;
import org.springframework.data.couchbase.core.query.N1qlPrimaryIndexed;

/**
 * Author test class for N1QL Join tests
 */
@N1qlPrimaryIndexed
public class Author {
    @Id
    String id;

    @Field("name")
    String name;

    @N1qlJoin(on = "lks.name=rks.authorName", fetchType = FetchType.IMMEDIATE)
    List<Book> books;

    public Author(String id, String name) {
        this.id = id;
        this.name = name;
    }

    public String getId() {
        return this.id;
    }

    public String getName() {
        return this.name;
    }

    public void setBooks(List<Book> books) {
        this.books = books;
    }

    public List<Book> getBooks() {
        return books;
    }
}