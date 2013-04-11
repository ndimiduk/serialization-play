package hbase;

import static java.lang.Integer.signum;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static types.VARCHAR.toBytes;
import static util.HSerializer.compare;
import static util.HSerializer.Order.ASCENDING;
import static util.HSerializer.Order.DESCENDING;

import java.util.Comparator;

import org.junit.Test;

import types.VARCHAR;

public class TestVARCHAR extends RandomTestHSerializable<String> {

  protected Comparator<String> getComparator() {
    return new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        return o1.compareTo(o2);
      }
    };
  }

  protected String create() {
    int len = r.nextInt(1024);
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
