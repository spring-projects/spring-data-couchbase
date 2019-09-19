package org.springframework.data.couchbase.repository;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.ContainerResourceRunner;
import org.springframework.data.couchbase.IntegrationTestApplicationConfig;
import org.springframework.data.couchbase.repository.config.RepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.support.CouchbaseRepositoryFactory;
import org.springframework.data.couchbase.repository.support.IndexManager;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;

/**
 * @author Simon Baslé
 * @author Mark Paluch
 */
@RunWith(ContainerResourceRunner.class)
@ContextConfiguration(classes = IntegrationTestApplicationConfig.class)
@TestExecutionListeners(PartyPopulatorListener.class)
public class QueryDerivationConversionIntegrationTests {

  @Autowired
  private Collection client;

  @Autowired
  private RepositoryOperationsMapping operationsMapping;

  @Autowired
  private IndexManager indexManager;

  private PartyRepository repository;

  @Before
  public void setup() throws Exception {
    RepositoryFactorySupport factory = new CouchbaseRepositoryFactory(operationsMapping, indexManager);
    repository = factory.getRepository(PartyRepository.class);
  }

  @Test
  public void testConvertsDateParameterInN1qlQuery() {
    Optional<Party> partyApril = repository.findById("testparty-3");
    assertTrue(partyApril.isPresent());

    Calendar cal = Calendar.getInstance();
    cal.clear();
    cal.set(2015, Calendar.APRIL, 10);
    Date find = cal.getTime();

    List<Party> parties = repository.findByEventDateIs(find);
    assertNotNull(parties);
    assertEquals(1, parties.size());
    assertEquals(find, parties.get(0).getEventDate());

    JsonObject doc = client.get(parties.get(0).getKey()).contentAsObject();
    assertEquals(find.getTime(), doc.get("eventDate"));
  }

  @Test
  public void testAcceptLongParameterInN1qlQuery() {
    List<Party> newYear90 = repository.findByAttendeesGreaterThanEqual(1200000);
    assertNotNull(newYear90);
    assertEquals(1, newYear90.size());
    assertEquals("aTestParty", newYear90.get(0).getKey());
  }

  @Test
  public void testConvertDateParameterInViewQuery() {
    Calendar cal = Calendar.getInstance();
    cal.clear();
    cal.set(2015, Calendar.AUGUST, 12);
    Date find = cal.getTime();

    List<Party> afterSummerParties = repository.findFirst3ByEventDateGreaterThanEqual(find);
    assertNotNull(afterSummerParties);
    assertEquals(3, afterSummerParties.size());
    for (Party afterSummerParty : afterSummerParties) {
      assert(afterSummerParty.getEventDate().after(find));
    }
  }
}
