package util;

import java.nio.ByteBuffer;
import java.util.Comparator;

import org.apache.hadoop.hbase.util.Bytes;

public abstract class HSerializer<T> {

  /**
   * The order in which a HSerializer implementation will sort, according to
   * the natural order of the underlying type.
   */
  public enum Order {
    ASCENDING  ((byte)0x00),
    DESCENDING ((byte)0xff);

    private final byte mask;

    /**
     * The bit-mask used to invert a value according to this order.
     */
    public byte mask() { return mask; }

    /**
     * Returns the adjusted trichotomous value according to the ordering
     * imposed by this <code>Order</code>.
     */
    public int cmp(int cmp) {
      return cmp * (this == ASCENDING ? 1 : -1);
    }

    Order(byte mask) { this.mask = mask; }
  }

  public static int compare(byte[] left, byte[] right) {
    return Bytes.compareTo(left, right);
  }

  public static int compare(byte[] left, int loffset, int llen, byte[] right,
      int roffset, int rlen) {
    return Bytes.compareTo(left, loffset, llen, right, roffset, rlen);
  }

  /**
   * Extend a {@link Comparable} to support nulls on either side.
   */
  public static <T extends Comparable<T>> int compare(HSerializer<T> serde, T t1, T t2) {
    return compare(serde.order, t1, t2);
  }

  /**
   * Extend a {@link Comparable} to support nulls on either side.
   */
  public static <T extends Comparable<T>> int compare(Order o, T t1, T t2) {
    if (t1 == null) {
      if (t2 == null) return 0;
      else return o.cmp(-1);
    }
    if (t2 == null) return o.cmp(1);
    return o.cmp(t1.compareTo(t2));
  }

  /**
   * Extend a {@link Comparator} to support nulls on either side.
   */
  public static <T> int compare(Comparator<T> cmp, HSerializer<T> serde, T t1, T t2) {
    return compare(cmp, serde.order, t1, t2);
  }

  /**
   * Extend a {@link Comparator} to support nulls on either side.
   */
  public static <T> int compare(Comparator<T> cmp, Order o, T t1, T t2) {
    if (t1 == null) {
      if (t2 == null) return 0;
      else return o.cmp(-1);
    }
    if (t2 == null) return o.cmp(1);
    return o.cmp(cmp.compare(t1, t2));
  }

  /**
   * Convert a byte into a String.
   */
  public static String toBinaryString(byte b) {
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

  /**
   * Get the sort <code>Order</code> specified by this HSerializer.
   */
  public Order order() { return order; }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "(" + order + ")";
  }

  public abstract boolean supportsNull();
  public abstract byte[] toBytes(T val);
  public abstract T fromBytes(byte[] bytes);
  public abstract void write(ByteBuffer buff, T val);
  public abstract T read(ByteBuffer buff);
}
