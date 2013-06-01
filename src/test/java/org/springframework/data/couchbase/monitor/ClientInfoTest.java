package org.springframework.data.couchbase.monitor;

import com.couchbase.client.CouchbaseClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.TestApplicationConfig;
import org.springframework.data.couchbase.monitor.ClientInfo;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertFalse;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestApplicationConfig.class)
public class ClientInfoTest {

  @Autowired
  private CouchbaseClient client;

  private ClientInfo ci;

  @Before
  public void setup() {
    ci = new ClientInfo(client);
  }

  @Test
  public void hostNames() {
    String hostnames = ci.getHostNames();
    assertNotNull(hostnames);
    assertFalse(hostnames.isEmpty());
  }

}
