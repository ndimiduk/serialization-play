package hbase;

import static java.lang.Integer.signum;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static types.VARCHAR.toBytes;
import static util.HSerializer.compareTo;
import static util.HSerializer.Order.ASCENDING;
import static util.HSerializer.Order.DESCENDING;

import org.junit.Test;

import types.VARCHAR;

public class TestVARCHAR {

  @Test
  public void testSerialize() {
    assertEquals(
      signum("foo".compareTo("bar")),
      signum(compareTo(toBytes("foo", ASCENDING), toBytes("bar", ASCENDING))));
    assertEquals(
      signum("foo".compareTo("bar")),
      signum(-compareTo(toBytes("foo", DESCENDING), toBytes("bar", DESCENDING))));
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
