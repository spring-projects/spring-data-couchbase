package org.springframework.data.couchbase;

import com.couchbase.client.ClusterManager;
import com.couchbase.client.clustermanager.BucketType;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultHttpClient;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.util.Arrays;

public class BucketCreator implements InitializingBean {

  private final String hostUri;
  private final String adminUser;
  private final String adminPass;

  public BucketCreator(String host, String user, String pass) {
    hostUri = host;
    adminUser = user;
    adminPass = pass;
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    BasicCredentialsProvider credentialsProvider =  new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(adminUser, adminPass));
    client.setCredentialsProvider(credentialsProvider);
    ClientHttpRequestFactory rf = new HttpComponentsClientHttpRequestFactory(client);

    RestTemplate template = new RestTemplate(rf);

    String fullUri = hostUri + "/default/buckets/default";

    try {
       template.getForEntity(fullUri, String.class);
    } catch (HttpClientErrorException ex) {
      if (ex.getMessage().equals("404 Object Not Found")) {
        createBucket();
      } else {
        throw new RuntimeException("Could not see if bucket is already created.", ex);
      }
    }
  }

  private void createBucket() throws Exception {
    ClusterManager bucketManager =
      new ClusterManager(Arrays.asList(new URI(hostUri)), adminUser, adminPass);
    bucketManager.createDefaultBucket(BucketType.COUCHBASE, 128, 0, true);
    Thread.sleep(5000);
    bucketManager.shutdown();
  }

}
