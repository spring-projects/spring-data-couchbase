package org.springframework.data.couchbase.config;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.couchbase.core.CouchbaseExceptionTranslator;

/**
 * The Factory Bean to help {@link CouchbaseClusterParser} constructing a {@link Cluster}
 *
 */
public class CouchbaseClusterFactoryBean extends AbstractFactoryBean<Cluster> implements PersistenceExceptionTranslator {

    private final String username;
    private final String password;
    private final String connectionString;

    private final PersistenceExceptionTranslator exceptionTranslator = new CouchbaseExceptionTranslator();

    public CouchbaseClusterFactoryBean(String connectionString, String username, String password) {
        this.connectionString = connectionString;
        this.username = username;
        this.password = password;
    }


    @Override
    public Class<?> getObjectType() {
        return Bucket.class;
    }

    @Override
    protected Cluster createInstance() {
        return Cluster.connect(connectionString, username, password);
    }

    @Override
    public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
        return exceptionTranslator.translateExceptionIfPossible(ex);
    }
}

