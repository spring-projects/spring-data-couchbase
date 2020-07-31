package org.springframework.data.couchbase;

import java.util.concurrent.Callable;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

/**
 * Helper to check if the Couchbase http endpoints are up.
 */
public class CouchbaseHttpPortListeningCheck implements Callable<Boolean> {

    private final int port;
    private final String path;

    public CouchbaseHttpPortListeningCheck(int port, String path) {
        this.port = port;
        this.path = path;
    }

    private Boolean executeRequest(URIBuilder builder) throws Exception {
        try {
            HttpGet request = new HttpGet(builder.build());
            HttpClient client = HttpClientBuilder.create().build();
            HttpResponse response = client.execute(request);
            int status = response.getStatusLine().getStatusCode();
            if (status < 200 || status >= 300) {
                return false;
            }
            return true;
        } catch (Exception ex) {
            Thread.sleep(1000);
            throw ex;
        }
    }

    @Override
    public Boolean call() throws Exception {
        URIBuilder builder = new URIBuilder();
        builder.setScheme("http").setHost("localhost").setPort(this.port).setPath(this.path);
        return executeRequest(builder);
    }
}
