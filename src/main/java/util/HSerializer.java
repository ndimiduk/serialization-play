package util;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.util.Bytes;

public abstract class HSerializer<T> {

  /**
   * The order in which a HSerializer implementation will sort, according to
   * the natural order of the underlying type.
   */
  public enum Order {
    ASCENDING  ((byte)0x00),
    DESCENDING ((byte)0xff);

    /**
     * The bit-mask used to invert a value according to this order.
     */
    public final byte mask;

    Order(byte mask) { this.mask = mask; }
  }

  public static int compareTo(byte[] left, byte[] right) {
    return Bytes.compareTo(left, right);
  }

  public static int compareTo(byte[] left, int loffset, int llen, byte[] right,
      int roffset, int rlen) {
    return Bytes.compareTo(left, loffset, llen, right, roffset, rlen);
  }

  /**
   * Convert a byte into a String.
   */
  private static String toBinaryString(byte b) {
    char[] buf = new char[8];
    byte[] ref = new byte[] { (byte) 0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02, 0x01 };
    for (byte i = 0; i < 8; i++) {
      buf[i] = (ref[i] & b) == 0 ? '0' : '1';
    }
    return new String(buf);
  }

  /**
   * Convert a byte[] into a String.
   */
  public static String toBinaryString(byte[] bs) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bs)
      sb.append(toBinaryString(b));
    return sb.toString();
  }

  public static final Order DEFAULT_ORDER = Order.ASCENDING;
  protected Order order = DEFAULT_ORDER;

  protected HSerializer() {}
  protected HSerializer(Order order) { this.order = order; }

  public abstract byte[] toBytes(T val);
  public abstract void putBytes(ByteBuffer buff, T val);
  public abstract T fromBytes(byte[] bytes);
}
