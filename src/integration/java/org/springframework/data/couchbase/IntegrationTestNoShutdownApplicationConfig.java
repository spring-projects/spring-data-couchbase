package org.springframework.data.couchbase;

import java.util.Collections;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.config.CouchbaseConfigurer;

/**
 * Configuration for testing no shutdown
 *
 * @author Subhashni Balakrishnan
 */
public class IntegrationTestNoShutdownApplicationConfig extends AbstractCouchbaseConfiguration {

    @Autowired
    private Environment springEnv;

    @Bean
    public String couchbaseAdminUser() {
        return springEnv.getProperty("couchbase.adminUser", "Administrator");
    }

    @Bean
    public String couchbaseAdminPassword() {
        return springEnv.getProperty("couchbase.adminUser", "password");
    }

    @Override
    protected List<String> getBootstrapHosts() {
        return Collections.singletonList(springEnv.getProperty("couchbase.host", "127.0.0.1"));
    }

    @Override
    protected String getBucketName() {
        return springEnv.getProperty("couchbase.bucket", "default");
    }

    @Override
    protected String getBucketPassword() {
        return springEnv.getProperty("couchbase.password", "");
    }

    @Override
    protected boolean isEnvironmentManagedBySpring() {
        return false;
    }

    @Override
    protected CouchbaseConfigurer couchbaseConfigurer() {
        return this;
    }
}