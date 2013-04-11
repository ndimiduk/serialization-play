package hbase;

import static java.lang.Integer.signum;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static util.HSerializer.compare;
import static util.HSerializer.Order.ASCENDING;
import static util.HSerializer.Order.DESCENDING;

import java.nio.ByteBuffer;
import java.util.Comparator;

import org.junit.Test;

import util.HSerializer;

public abstract class TestHSerializable<T> {

  protected abstract T create();
  protected abstract HSerializer<T> ascendingSerializer();
  protected abstract HSerializer<T> descendingSerializer();
  protected abstract Comparator<T> getComparator();

  protected void testSerialization(T val, HSerializer<T> serde) {

    // test byte[] serialization
    byte[] bytes = serde.toBytes(val);
    T p = serde.fromBytes(bytes);
    assertEquals(
      "round-trip byte[] serialization should be equal.",
      0, compare(getComparator(), serde, val, p));

    // test ByteBuffer serialization
    ByteBuffer buf = ByteBuffer.allocate(bytes.length);
    serde.write(buf, val);
    buf.flip();
    p = serde.read(buf);
    assertEquals("round-trip ByteBuffer serialization should be equal",
      0, compare(getComparator(), serde, val, p));
  }

  protected void testSort(T val1, T val2, HSerializer<T> serde) {
    byte[] bytes1 = serde.toBytes(val1);
    byte[] bytes2 = serde.toBytes(val2);
    int expectedOrder = signum(compare(getComparator(), serde, val1, val2));
    int byteOrder = signum(compare(bytes1, bytes2));

    assertEquals(
      String.format("%s sort order broken for <%s>, <%s>", serde.order(), val1, val2),
      expectedOrder, byteOrder);
  }

  @Test
  public void testHSerializable() {
    T o1 = create(), o2 = create();
    HSerializer<T> asc = ascendingSerializer();
    HSerializer<T> dsc = descendingSerializer();

    testSerialization(o1, asc);
    testSerialization(o1, dsc);
    testSort(o1, o2, asc);
    testSort(o1, o2, dsc);
  }

  @Test
  public void testNullAwareCompareAsc() {
    assertTrue(0 == compare(ASCENDING, (Long) null, null));
    assertTrue(0 > compare(ASCENDING, null, 1L));
    assertTrue(0 < compare(ASCENDING, 1L, null));
    assertTrue(0 > compare(ASCENDING, 1L, 2L));
  }

  @Test
  public void testNullAwareCompareDsc() {
    assertTrue(0 == compare(DESCENDING, (Long) null, null));
    assertTrue(0 < compare(DESCENDING, null, 1L));
    assertTrue(0 > compare(DESCENDING, 1L, null));
    assertTrue(0 < compare(DESCENDING, 1L, 2L));
  }
}
