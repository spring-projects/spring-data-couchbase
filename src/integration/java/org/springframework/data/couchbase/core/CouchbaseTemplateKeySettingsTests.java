package org.springframework.data.couchbase.core;

import static org.junit.Assert.*;
import com.couchbase.client.java.repository.annotation.Id;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.data.couchbase.ContainerResourceRunner;
import org.springframework.data.couchbase.IntegrationTestCustomKeySettings;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.core.mapping.KeySettings;
import org.springframework.test.context.ContextConfiguration;

/**
 * @author Subhashni Balakrishnan
 */

@RunWith(ContainerResourceRunner.class)
@ContextConfiguration(classes = IntegrationTestCustomKeySettings.class)
public class CouchbaseTemplateKeySettingsTests {

    @Rule
    public TestName testName = new TestName();

    @Autowired
    private CouchbaseTemplate template;

    @Before
    public void setup() {
        if (template.keySettings() == null) {
            template.keySettings(KeySettings.build().prefix("MyAppPrefix").suffix("MyAppSuffix").delimiter("::"));
        }
    }

    @Test
    public void shouldAddCustomKeySettings() throws Exception {
        SimpleClass simpleClass = new SimpleClass();
        String generatedId = template.getGeneratedId(simpleClass);
        assertEquals("Id generated should include custom key settings", "MyAppPrefix::myId::MyAppSuffix", generatedId);
    }

    @Test
    public void shouldNotAllowKeySettingsToBeChanged() {
        try {
            template.keySettings(KeySettings.build().prefix("MyAppPrefix").suffix("MyAppSuffix").delimiter("::"));
            fail("excepted unsupportedOperationException");
        } catch(Exception ex) {

        }
    }

    @Document
    static class SimpleClass {
        @Id
        public String id = "myId";
    }
}
