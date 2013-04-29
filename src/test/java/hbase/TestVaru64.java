package hbase;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import util.HSerializer;

public class TestVaru64 {

  /**
   */
  @Test
  public void testVaru64() {
  }

  /**
   * Building sqlite4 with -DVARINT_TOOL provides this reference:
   * $ ./varint_tool 240 2287 67823 16777215 4294967295 1099511627775 281474976710655 72057594037927935 18446744073709551615
   *                  240 = f0
   *                 2287 = f8ff
   *                67823 = f9ffff
   *             16777215 = faffffff
   *           4294967295 = fbffffffff
   *        1099511627775 = fcffffffffff
   *      281474976710655 = fdffffffffffff
   *    72057594037927935 = feffffffffffffff
   *  9223372036854775807 = ff7fffffffffffffff (Long.MAX_VAL)
   * 18446744073709551615 = ffffffffffffffffff
   * 
   * @see http://sqlite.org/src4/doc/trunk/www/varint.wiki
   */
  @Test
  public void testVaru64Boundaries() {
    byte[] buf = new byte[9];
    int len;

    long boundries[] = { 240L, 2287L, 67823L, 16777215L, 4294967295L, 1099511627775L, 281474976710655L, 72057594037927935L, -1L };
    String hexes[] = { "f0", "f8ff", "f9ffff", "faffffff", "fbffffffff", "fcffffffffff", "fdffffffffffff", "feffffffffffffff", "ffffffffffffffffff" };
    int byte_lens[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    assertEquals("Broken test!", boundries.length, hexes.length);
    assertEquals("Broken test!", boundries.length, byte_lens.length);

    for (int i = 0; i < boundries.length; i++) {
      len = HSerializer.putVaruint64(buf, 0, boundries[i]);
      assertEquals("Surprising serialized length.", byte_lens[i], len);
      assertEquals(
        String.format("%d serilized to: %s | %s.", boundries[i],
          HSerializer.toHexString(buf, 0, len), HSerializer.toBinaryString(buf, 0, len)),
        hexes[i], HSerializer.toHexString(buf, 0, len));
      assertEquals("Deserialization failed.", boundries[i], HSerializer.getVaruint64(buf, 0));
      assertEquals("Length inspection failed.", byte_lens[i], HSerializer.lengthVaru64(buf, 0));
    }
  }
}
