package org.springframework.data.couchbase.core;

/**
 * Test interface for projecting data from {@link CouchbaseTemplate}.
 *
 * @author Subhashni Balakrishnan
 */
public interface BeerProjection {
    String getDescription();
}
