package org.springframework.data.couchbase.repository;

import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.view.DefaultView;
import com.couchbase.client.java.view.DesignDocument;
import com.couchbase.client.java.view.View;

import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;

/**
 * @author Simon Baslé
 */
public class QueryDerivationConversionListener extends DependencyInjectionTestExecutionListener {

  @Override
  public void beforeTestClass(final TestContext testContext) throws Exception {
    Bucket client = (Bucket) testContext.getApplicationContext().getBean("couchbaseBucket");
    populateTestData(client);
    createAndWaitForDesignDocs(client);
  }

  private void populateTestData(Bucket client) {
    CouchbaseTemplate template = new CouchbaseTemplate(client);

    Calendar cal = Calendar.getInstance();
    cal.clear();
    cal.set(Calendar.YEAR, 2015);
    cal.set(Calendar.DAY_OF_MONTH, 10);
    cal.set(Calendar.MONTH, Calendar.JANUARY);
    for (int i = 0; i < 12; i++) {
      Party p = new Party("testparty-" + i, "party like it's 199" + i,
          "An awesome party, 90's themed, every 10 of the month",
          cal.getTime(), 100 + i * 10);
      template.save(p, PersistTo.MASTER, ReplicateTo.NONE);
      cal.roll(Calendar.MONTH, true);
    }

    cal.clear();
    cal.set(Calendar.YEAR, 1990);
    cal.set(Calendar.MONTH, Calendar.JANUARY);
    cal.set(Calendar.DAY_OF_MONTH, 01);
    template.save(new Party("aTestParty", "New Year's Eve 90", "Happy New Year", cal.getTime(), 1230000));
  }

  private void createAndWaitForDesignDocs(Bucket client) {
    String mapFunction = "function (doc, meta) { if(doc._class == \"" + Party.class.getName() + "\") { emit(doc.eventDate, null); } }";
    View view = DefaultView.create("byDate", mapFunction, "_count");
    List<View> views = Collections.singletonList(view);
    DesignDocument designDoc = DesignDocument.create("party", views);
    client.bucketManager().upsertDesignDocument(designDoc);
  }
}
