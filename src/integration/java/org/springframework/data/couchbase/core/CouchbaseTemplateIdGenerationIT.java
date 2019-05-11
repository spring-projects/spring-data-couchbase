package org.springframework.data.couchbase.core;

import static org.junit.Assert.*;
import static org.springframework.data.couchbase.core.mapping.id.GenerationStrategy.*;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.annotation.Id;
import org.springframework.data.couchbase.ContainerResourceRunner;
import org.springframework.data.couchbase.IntegrationTestApplicationConfig;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.core.mapping.id.GeneratedValue;
import org.springframework.data.couchbase.core.mapping.id.IdAttribute;
import org.springframework.data.couchbase.core.mapping.id.IdPrefix;
import org.springframework.data.couchbase.core.mapping.id.IdSuffix;
import org.springframework.test.context.ContextConfiguration;

/**
 * @author Subhashni Balakrishnan
 */
@RunWith(ContainerResourceRunner.class)
@ContextConfiguration(classes = IntegrationTestApplicationConfig.class)
public class CouchbaseTemplateIdGenerationIT {

    @Rule
    public TestName testName = new TestName();

    @Autowired
    private Bucket client;

    @Autowired
    private CouchbaseTemplate template;


    private void removeIfExist(String key) {
        try {
            client.remove(key);
        }
        catch (DocumentDoesNotExistException e) {
            //ignore
        }
    }

    @Test
    public void shouldGenerateIdUsingAtrributes() throws Exception {
        SimpleClassWithGeneratedIdValueUsingAttributes simpleClass = new SimpleClassWithGeneratedIdValueUsingAttributes();
        String generatedId = template.getGeneratedId(simpleClass);

        removeIfExist(generatedId);
        assertEquals("Id generation should be correct", generatedId,
                "prefix1::prefix2::0::1::2.0::3.0::4::Simple::Nested{value:simple}::suffix1::suffix2");
        template.insert(simpleClass);
        assertEquals("Exists after insert", true, template.exists(generatedId));
        simpleClass.value = "modified";
        template.save(simpleClass);
        SimpleClassWithGeneratedIdValueUsingAttributes modifiedClass = template.findById(generatedId,
                SimpleClassWithGeneratedIdValueUsingAttributes.class);
        assertEquals("Get after save id should be correct", generatedId, modifiedClass.id);
        template.update(simpleClass);
        SimpleClassWithGeneratedIdValueUsingAttributes updatedClass = template.findById(generatedId,
                SimpleClassWithGeneratedIdValueUsingAttributes.class);
        assertEquals("Get after update id should be correct", generatedId, updatedClass.id);
        template.remove(generatedId);
        assertEquals("Exists after remove", false, template.exists(generatedId));
    }

    @Test
    public void shouldGenerateIdUsingUUID() throws Exception {
        SimpleClassWithGeneratedIdValueUsingUUID simpleClass = new SimpleClassWithGeneratedIdValueUsingUUID();
        String generatedId = template.getGeneratedId(simpleClass);
        simpleClass.id = generatedId;
        template.insert(simpleClass);
        assertEquals("Should not regenerate id", generatedId, simpleClass.id);
        template.remove(generatedId);
        assertEquals("Exists after remove", false, template.exists(generatedId));
    }


    @Document
    static class SimpleClassWithGeneratedIdValueUsingAttributes {

        @Id @GeneratedValue(strategy = USE_ATTRIBUTES, delimiter = "::")
        public String id;

        @IdAttribute(order = 6)
        public Nested nested = new Nested("simple");

        @IdAttribute(order = 5)
        public String type = "Simple";

        @IdAttribute(order = 4)
        public int intNum = 4;

        @IdAttribute(order = 2)
        public float floatNum = 2F;

        @IdAttribute(order = 3)
        public double doubleNum = 3;

        @IdAttribute
        public long longNum = 0L;

        @IdAttribute(order = 1)
        public short shortNum = 1;

        @IdPrefix(order = 1)
        public String prefix2 = "prefix2";

        @IdPrefix
        public String prefix1 = "prefix1";

        @IdSuffix(order = 1)
        public String suffix2 = "suffix2";

        @IdSuffix
        public String suffix1 = "suffix1";

        public String value = "new";

    }

    static class Nested {
        private String value;

        public Nested(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "Nested{value:" + value + "}";
        }
    }

    @Document
    static class SimpleClassWithGeneratedIdValueUsingUUID {
        @Id @GeneratedValue(strategy = UNIQUE)
        public String id;

        public String value = "new";
    }
}
