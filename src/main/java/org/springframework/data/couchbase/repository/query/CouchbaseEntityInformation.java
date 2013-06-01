package org.springframework.data.couchbase.repository.query;

import org.springframework.data.repository.core.EntityInformation;

import java.io.Serializable;


public interface CouchbaseEntityInformation<T, ID extends Serializable> extends EntityInformation<T, ID> {
}
