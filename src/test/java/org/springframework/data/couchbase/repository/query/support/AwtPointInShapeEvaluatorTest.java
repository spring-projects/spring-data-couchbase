package org.springframework.data.couchbase.repository.query.support;

import static org.assertj.core.api.Assertions.assertThat;

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
