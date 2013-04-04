package hbase;

import static util.HSerializer.Order.ASCENDING;

import org.junit.Test;

import types.CHAR;

public class TestCHAR {

  @Test(expected = IllegalArgumentException.class)
  public void testSerializeConstraints1() {
    new CHAR(0, ASCENDING).toBytes("foo");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSerializeConstraints2() {
    new CHAR(3, ASCENDING).toBytes("foo");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDeserializeConstraints1() {
    new CHAR(1, ASCENDING).fromBytes(new byte[] { 0, 1 });
  }
}
