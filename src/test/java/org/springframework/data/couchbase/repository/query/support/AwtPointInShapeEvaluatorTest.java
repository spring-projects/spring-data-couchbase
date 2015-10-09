package org.springframework.data.couchbase.repository.query.support;

/**
 * Test case for the {@link AwtPointInShapeEvaluator}.
 *
 * @author Simon Baslé
 */
public class AwtPointInShapeEvaluatorTest extends AbstractPointInShapeEvaluatorTest {

  @Override
  public PointInShapeEvaluator createEvaluator() {
    return new AwtPointInShapeEvaluator();
  }
}
