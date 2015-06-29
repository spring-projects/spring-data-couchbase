package org.springframework.data.couchbase;

import java.util.Collections;
import java.util.List;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseBucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import org.mockito.Mock;
import org.mockito.Mockito;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.WriteResultChecking;

@Configuration
public class UnitTestApplicationConfig extends AbstractCouchbaseConfiguration {

	@Bean
	public String couchbaseAdminUser() {
		return "someLogin";
	}

	@Bean
	public String couchbaseAdminPassword() {
		return "somePassword";
	}

	@Override
	protected List<String> getBootstrapHosts() {
		return Collections.singletonList("192.1.2.3");
	}

	@Override
	protected String getBucketName() {
		return "someBucket";
	}

	@Override
	protected String getBucketPassword() {
		return "someBucketPassword";
	}

	@Override
	public Cluster couchbaseCluster() throws Exception {
		return Mockito.mock(CouchbaseCluster.class);
	}

	@Override
	public Bucket couchbaseClient() throws Exception {
		return Mockito.mock(CouchbaseBucket.class);
	}

	@Override
	public CouchbaseTemplate couchbaseTemplate() throws Exception {
		CouchbaseTemplate template = super.couchbaseTemplate();
		template.setWriteResultChecking(WriteResultChecking.LOG);
		return template;
	}
}
