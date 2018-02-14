package org.springframework.data.couchbase;

import org.junit.ClassRule;
import org.junit.runners.model.InitializationError;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * This runner initializes container for the container based testing.
 *
 * @author Subhashni Balakrishnan
 */
public class ContainerResourceRunner extends SpringJUnit4ClassRunner {

    @ClassRule
    public static final TestContainerResource resource = TestContainerResource.getResource();

    public ContainerResourceRunner(Class<?> clazz) throws InitializationError {
        super(clazz);
    }
}
