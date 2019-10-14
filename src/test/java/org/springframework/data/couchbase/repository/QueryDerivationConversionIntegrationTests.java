package org.springframework.data.couchbase.repository;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Simon Baslé
 * @author Mark Paluch
 */
@RunWith(ContainerResourceRunner.class)
@ContextConfiguration(classes = IntegrationTestApplicationConfig.class)
@TestExecutionListeners(PartyPopulatorListener.class)
public class QueryDerivationConversionIntegrationTests {

  @Autowired
  private Bucket client;

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
    assertThat(partyApril.isPresent()).isTrue();

    Calendar cal = Calendar.getInstance();
    cal.clear();
    cal.set(2015, Calendar.APRIL, 10);
    Date find = cal.getTime();

    List<Party> parties = repository.findByEventDateIs(find);
    assertThat(parties).isNotNull();
    assertThat(parties.size()).isEqualTo(1);
    assertThat(parties.get(0).getEventDate()).isEqualTo(find);

    JsonDocument doc = client.get(parties.get(0).getKey());
    assertThat(doc.content().get("eventDate")).isEqualTo(find.getTime());
  }

  @Test
  public void testAcceptLongParameterInN1qlQuery() {
    List<Party> newYear90 = repository.findByAttendeesGreaterThanEqual(1200000);
    assertThat(newYear90).isNotNull();
    assertThat(newYear90.size()).isEqualTo(1);
    assertThat(newYear90.get(0).getKey()).isEqualTo("aTestParty");
  }

  @Test
  public void testConvertDateParameterInViewQuery() {
    Calendar cal = Calendar.getInstance();
    cal.clear();
    cal.set(2015, Calendar.AUGUST, 12);
    Date find = cal.getTime();

    List<Party> afterSummerParties = repository.findFirst3ByEventDateGreaterThanEqual(find);
    assertThat(afterSummerParties).isNotNull();
    assertThat(afterSummerParties.size()).isEqualTo(3);
    for (Party afterSummerParty : afterSummerParties) {
      assert(afterSummerParty.getEventDate().after(find));
    }
  }
}
