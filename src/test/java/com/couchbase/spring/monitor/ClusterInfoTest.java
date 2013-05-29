package com.couchbase.spring.monitor;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.spring.TestApplicationConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestApplicationConfig.class)
public class ClusterInfoTest {

  @Autowired
  private CouchbaseClient client;

  private ClusterInfo ci;

  @Before
  public void setup() {
    ci = new ClusterInfo(client);
  }

  @Test
  public void totalRAMAssigned() {
    assertTrue(ci.getTotalRAMAssigned() > 0);
  }

  @Test
  public void totalRAMUsed() {
    assertTrue(ci.getTotalRAMUsed() > 0);
  }

}
