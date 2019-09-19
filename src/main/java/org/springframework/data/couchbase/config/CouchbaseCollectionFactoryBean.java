package org.springframework.data.couchbase.config;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.couchbase.core.CouchbaseExceptionTranslator;

public class CouchbaseCollectionFactoryBean extends AbstractFactoryBean<Collection> implements PersistenceExceptionTranslator {

    private final Cluster cluster;
    private final String bucketName;
    private final String collectionName;

    private final PersistenceExceptionTranslator exceptionTranslator = new CouchbaseExceptionTranslator();

    public CouchbaseCollectionFactoryBean(Cluster cluster) {
        this(cluster, "default", null);
    }

    public CouchbaseCollectionFactoryBean(Cluster cluster, String bucketName) {
        this(cluster, bucketName, null);
    }

    public CouchbaseCollectionFactoryBean(Cluster cluster, String bucketName, String collectionName) {
        this.cluster = cluster;
        this.collectionName = collectionName;
        if (bucketName == null || bucketName.isEmpty()) {
            this.bucketName = "default";
        } else {
            this.bucketName = bucketName;
        }
    }

    @Override
    public Class<?> getObjectType() {
        return Collection.class;
    }

    @Override
    protected Collection createInstance() throws Exception {
        if (collectionName == null || collectionName.isEmpty()) {
            return cluster.bucket(bucketName).defaultCollection();
        }
        return cluster.bucket(bucketName).collection(collectionName);
    }

    @Override
    public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
        return exceptionTranslator.translateExceptionIfPossible(ex);
    }
}
