package hbase;

import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;

/**
 * Base-class providing repeatable random generation of test values.
 */
public abstract class RandomTestHSerializable<T extends Comparable<T>> extends TestHSerializable<T> {

  protected static final Log LOG = LogFactory.getLog(RandomTestHSerializable.class);

  protected Random r;
  protected int numTests;

  /**
   * Initialize <code>Random</code> and <code>numTests</code>.
   */
  @Before
  public void setUp() {
    String seed = System.getProperty("test.random.seed", "" + System.currentTimeMillis());
    LOG.info("Using test.random.seed value: " + seed);
    r = new Random(Long.valueOf(seed));
    numTests = Integer.valueOf(System.getProperty("test.random.count", "8192"));
    LOG.info("Using test.random.count value: " + numTests);
  }

  /**
   * Run <code>testHSerializable</code> <code>numTests</code> times.
   */
  @Test
  @Override
  public void testHSerializable() {
    for (int i = 0; i < numTests; i++) {
      super.testHSerializable();
    }
  }
}
