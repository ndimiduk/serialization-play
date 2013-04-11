package hbase;

import static java.lang.Integer.signum;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static types.LONG.toBytes;
import static types.LONG.toLong;
import static util.HSerializer.compare;
import static util.HSerializer.Order.ASCENDING;
import static util.HSerializer.Order.DESCENDING;

import java.util.Comparator;

import org.junit.Test;

import types.LONG;

public class TestLONG extends RandomTestHSerializable<Long> {

  private static final byte ONES = (byte) 0xFF;

  protected Comparator<Long> getComparator() {
    return new Comparator<Long>() {
      @Override
      public int compare(Long o1, Long o2) {
        return o1.compareTo(o2);
      }
    };
  }

  protected Long create() {
    return r.nextLong();
  }

  protected LONG ascendingSerializer() { return new LONG(ASCENDING); }
  protected LONG descendingSerializer() { return new LONG(DESCENDING); }

  @Test
  public void testSerialize() {
    assertArrayEquals(
      new byte[] { (byte) 0x80, 0, 0, 0, 0, 0, 0, 0x01 },
      toBytes(1L));
    assertArrayEquals(
      new byte[] { 0x7f, ONES, ONES, ONES, ONES, ONES, ONES, ONES },
      toBytes(-1));
    assertEquals(
      signum(new Long(1L).compareTo(0L)),
      signum(compare(toBytes(1L, ASCENDING), toBytes(0L, ASCENDING))));
    assertEquals(
      signum(new Long(1L).compareTo(0L)),
      signum(-compare(toBytes(1L, DESCENDING), toBytes(0L, DESCENDING))));
  }

  @Test
  public void testDeserialize() {
    long posExpected = 1L;
    byte[] posInput = new byte[] { (byte) 0x80, 0, 0, 0, 0, 0, 0, 0x01 };
    assertEquals(
      posExpected,
      toLong(posInput));

    long negExpected = -1L;
    byte[] negInput = new byte[] { 0x7f, ONES, ONES, ONES, ONES, ONES, ONES, ONES };
    assertEquals(
      negExpected,
      toLong(negInput));
  }

  @Test
  public void testRoundTrip() {
    assertEquals(0L, toLong(toBytes(0L)));
    assertEquals(1L, toLong(toBytes(1L)));
    assertEquals(-1L, toLong(toBytes(-1L)));
    assertEquals(Long.MIN_VALUE, toLong(toBytes(Long.MIN_VALUE)));
    assertEquals(Long.MAX_VALUE, toLong(toBytes(Long.MAX_VALUE)));
  }
}
