package hbase;

import static java.lang.Integer.signum;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static types.BOOLEAN.toBoolean;
import static types.BOOLEAN.toBytes;
import static util.HSerializer.compare;
import static util.HSerializer.Order.ASCENDING;
import static util.HSerializer.Order.DESCENDING;

import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import types.BOOLEAN;

public class TestBOOLEAN extends TestHSerializable<Boolean> {

  protected static final Log LOG = LogFactory.getLog(TestBOOLEAN.class);
  private static final Random r;

  static {
    String seed = System.getProperty("test.random.seed", "" + System.currentTimeMillis());
    LOG.info("Using random seed: " + seed);
    r = new Random(Long.valueOf(seed));
  }

  protected Boolean create() {
    return r.nextBoolean();
  }

  protected BOOLEAN ascendingSerializer() { return new BOOLEAN(ASCENDING); }
  protected BOOLEAN descendingSerializer() { return new BOOLEAN(DESCENDING); }

  @Test
  public void testSerialize() {
    assertArrayEquals(new byte[] { (byte) 0xFF }, toBytes(true));
    assertArrayEquals(new byte[] { (byte) 0x01 }, toBytes(false));
    assertArrayEquals(new byte[] { (byte) 0x00 }, new BOOLEAN().toBytes(null));
    assertEquals(
      signum(Boolean.TRUE.compareTo(Boolean.FALSE)),
      signum(compare(toBytes(true, ASCENDING), toBytes(false, ASCENDING))));
    assertEquals(
      signum(Boolean.TRUE.compareTo(Boolean.FALSE)),
      signum(-compare(toBytes(true, DESCENDING), toBytes(false, DESCENDING))));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDeserializeJunk() {
    toBoolean(new byte[] { 0x02 });
  }

  @Test
  public void testDeserialize() {
    assertEquals(true, toBoolean(new byte[] { (byte) 0xFF }));
    assertEquals(false, toBoolean(new byte[] { (byte) 0x01 }));
    assertEquals(null, new BOOLEAN().fromBytes(new byte[] { 0x00 }));
  }

  @Test
  public void testRoundTrip() {
    assertEquals(true, toBoolean(toBytes(true)));
    assertEquals(false, toBoolean(toBytes(false)));
    BOOLEAN b = new BOOLEAN();
    assertEquals(null, b.fromBytes(b.toBytes(null)));
  }
}
