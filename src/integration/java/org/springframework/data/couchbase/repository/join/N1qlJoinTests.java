package org.springframework.data.couchbase.repository.join;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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
 * N1ql Join tests
 *
 * @author Subhashni Balakrishnan
 */
@RunWith(ContainerResourceRunner.class)
@ContextConfiguration(classes = IntegrationTestApplicationConfig.class)
@TestExecutionListeners(listeners = {AuthorAndBookPopulatorListener.class})
public class N1qlJoinTests {

    @Autowired
    private RepositoryOperationsMapping operationsMapping;

    @Autowired
    private IndexManager indexManager;

    private BookRepository bookRepository;

    private AuthorRepository authorRepository;

    @Before
    public void setup() throws Exception {
        RepositoryFactorySupport factory = new CouchbaseRepositoryFactory(operationsMapping, indexManager);
        bookRepository = factory.getRepository(BookRepository.class);
        authorRepository = factory.getRepository(AuthorRepository.class);
    }

    @Test
    public void testN1qlJoin() {
        Author a = authorRepository.findById("Author" + 1).get();
        assertTrue(a.books.size() == 5);
        for(Book b:a.books) {
            assertEquals("Join on author name mismatch", a.name, b.authorName);
        }
    }

    @Test
    public void testN1qlJoinWithNoResults() {
        final String name = "testN1qlJoinWithNoResults";
        Author a = new Author(name, name);
        authorRepository.save(a);

        Author saveda = authorRepository.findById(name).get();
        assertTrue(saveda.books.isEmpty());
    }
}