package org.springframework.data.couchbase.core;

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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * @author Subhashni Balakrishnan
 */

@RunWith(ContainerResourceRunner.class)
@ContextConfiguration(classes = IntegrationTestCustomKeySettings.class)
public class CouchbaseTemplateKeySettingsIntegrationTests {

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
        assertThat(generatedId).as("Id generated should include custom key settings")
                .isEqualTo("MyAppPrefix::myId::MyAppSuffix");
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
