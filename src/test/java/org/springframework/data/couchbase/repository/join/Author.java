/*
 * Copyright 2012-2019 the original author or authors
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
package org.springframework.data.couchbase.repository.join;
import java.util.List;

import org.springframework.data.annotation.Id;
import org.springframework.data.couchbase.core.mapping.annotation.Field;
import org.springframework.data.couchbase.core.query.FetchType;
import org.springframework.data.couchbase.core.query.N1qlJoin;
import org.springframework.data.couchbase.core.query.N1qlPrimaryIndexed;

/**
 * Author test class for N1QL Join tests
 *
 * @author Tayeb Chlyah
 */
@N1qlPrimaryIndexed
public class Author {
    @Id
    String id;

    @Field("name")
    String name;

    @N1qlJoin(on = "lks.name=rks.authorName", fetchType = FetchType.IMMEDIATE)
    List<Book> books;

    @N1qlJoin(on = "lks.name=rks.name", fetchType = FetchType.IMMEDIATE)
    Address address;

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

    public Address getAddress() {
        return address;
    }

    public void setAddress(Address address) {
        this.address = address;
    }
}