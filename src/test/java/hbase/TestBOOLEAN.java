package hbase;

import static java.lang.Integer.signum;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static types.BOOLEAN.toBoolean;
import static types.BOOLEAN.toBytes;
import static util.HSerializer.compareTo;
import static util.HSerializer.Order.ASCENDING;
import static util.HSerializer.Order.DESCENDING;

import org.junit.Test;

public class TestBOOLEAN {

  @Test
  public void testSerialize() {
    assertArrayEquals(new byte[] { (byte) 0xFF }, toBytes(true));
    assertArrayEquals(new byte[] { (byte) 0x00 }, toBytes(false));
    assertEquals(
      signum(Boolean.TRUE.compareTo(Boolean.FALSE)),
      signum(compareTo(toBytes(true, ASCENDING), toBytes(false, ASCENDING))));
    assertEquals(
      signum(Boolean.TRUE.compareTo(Boolean.FALSE)),
      signum(-compareTo(toBytes(true, DESCENDING), toBytes(false, DESCENDING))));
  }

  @Test
  public void testDeserialize() {
    assertEquals(true, toBoolean(new byte[] { (byte) 0xFF }));
    assertEquals(false, toBoolean(new byte[] { (byte) 0x00 }));
  }

  @Test
  public void testRoundTrip() {
    assertEquals(true, toBoolean(toBytes(true)));
    assertEquals(false, toBoolean(toBytes(false)));
  }
}
