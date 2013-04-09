package hbase;

import static java.lang.Integer.signum;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static types.VARCHAR.toBytes;
import static util.HSerializer.compare;
import static util.HSerializer.Order.ASCENDING;
import static util.HSerializer.Order.DESCENDING;

import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import types.VARCHAR;

public class TestVARCHAR extends TestHSerializable<String> {

  protected static final Log LOG = LogFactory.getLog(TestVARCHAR.class);
  private static final Random r;

  static {
    String seed = System.getProperty("test.random.seed", "" + System.currentTimeMillis());
    LOG.info("Using random seed: " + seed);
    r = new Random(Long.valueOf(seed));
  }

  protected String create() {
    int len = r.nextInt(4); // 1024
    StringBuilder sb = new StringBuilder(len);

    for (int i = 0; i < len; i++)
      sb.appendCodePoint(r.nextInt(Character.MAX_CODE_POINT + 1));
    return sb.toString();
  }

  protected VARCHAR ascendingSerializer() { return new VARCHAR(ASCENDING); }
  protected VARCHAR descendingSerializer() { return new VARCHAR(DESCENDING); }

  @Test
  public void testSerialize() {
    assertEquals(
      signum("foo".compareTo("bar")),
      signum(compare(toBytes("foo", ASCENDING), toBytes("bar", ASCENDING))));
    assertEquals(
      signum("foo".compareTo("bar")),
      signum(-compare(toBytes("foo", DESCENDING), toBytes("bar", DESCENDING))));
  }

  @Test
  public void testRoundTrip() {
    VARCHAR asc = new VARCHAR(ASCENDING);
    assertEquals(null, asc.fromBytes(asc.toBytes(null)));
    assertEquals("", asc.fromBytes(asc.toBytes("")));
    assertArrayEquals("foo".toCharArray(), asc.fromBytes(asc.toBytes("foo")).toCharArray());

    VARCHAR dsc = new VARCHAR(DESCENDING);
    assertEquals(null, dsc.fromBytes(dsc.toBytes(null)));
    assertEquals("", dsc.fromBytes(dsc.toBytes("")));
    assertArrayEquals("foo".toCharArray(), dsc.fromBytes(dsc.toBytes("foo")).toCharArray());
  }
}
