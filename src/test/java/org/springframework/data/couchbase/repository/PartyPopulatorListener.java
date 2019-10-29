package org.springframework.data.couchbase.repository;
import java.util.Calendar;

import com.couchbase.client.java.Bucket;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import org.springframework.data.couchbase.config.BeanNames;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.geo.Point;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;

/**
 * @author Simon Baslé
 */
public class PartyPopulatorListener extends DependencyInjectionTestExecutionListener {

  @Override
  public void beforeTestClass(final TestContext testContext) throws Exception {
    Cluster cluster = (Cluster) testContext.getApplicationContext().getBean(BeanNames.COUCHBASE_CLUSTER);
    Collection collection = (Collection) testContext.getApplicationContext().getBean(BeanNames.COUCHBASE_COLLECTION);
    populateTestData(cluster, collection);
  }

  private void populateTestData(Cluster cluster, Collection collection) {
    CouchbaseTemplate template = new CouchbaseTemplate(cluster, collection);

    Calendar cal = Calendar.getInstance();
    cal.clear();
    cal.set(Calendar.YEAR, 2015);
    cal.set(Calendar.DAY_OF_MONTH, 10);
    cal.set(Calendar.MONTH, Calendar.JANUARY);
    for (int i = 0; i < 12; i++) {
      Party p = new Party("testparty-" + i, "party like it's 199" + i,
          "An awesome party, 90's themed, every 10 of the month",
          cal.getTime(), 100 + i * 10,
          new Point(i, -i));
      template.save(p, PersistTo.ACTIVE, ReplicateTo.NONE);
      cal.roll(Calendar.MONTH, true);
    }

    cal.clear();
    cal.set(Calendar.YEAR, 1990);
    cal.set(Calendar.MONTH, Calendar.JANUARY);
    cal.set(Calendar.DAY_OF_MONTH, 01);
    template.save(new Party("aTestParty", "New Year's Eve 90", "Happy New Year", cal.getTime(), 1230000, new Point(100, 100)));
    template.save(new Party("lowercaseParty", "lowercase party", "lowercase party", cal.getTime(), 1000, new Point(100, 100)));
    template.save(new Party("uppercaseParty", "Uppercase party", "Uppercase party", cal.getTime(), 1000, new Point(100, 100)));
  }

}
