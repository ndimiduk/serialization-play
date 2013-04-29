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
    return toBinaryString(bs, 0, bs.length);
  }

  /**
   * Convert a byte[] into a String.
   */
  public static String toBinaryString(byte[] bs, int offset, int length) {
    assert bs.length >= length;
    assert offset >= 0 && offset <= bs.length - 1;

    StringBuilder sb = new StringBuilder();
    for (int i = offset; i < length; i++) {
      sb.append(toBinaryString(bs[i])).append(" ");
    }
    return sb.toString();
  }

  public static String toHexString(byte[] bs) {
    return toHexString(bs, 0, bs.length);
  }

  public static String toHexString(byte[] bs, int offset, int length) {
    assert bs.length >= length;
    assert offset >= 0 && offset <= bs.length - 1;

    StringBuilder sb = new StringBuilder();
    for (int i = offset; i < length; i++) {
      sb.append(String.format("%02x", bs[i]));
    }
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

  public static int unsignedCmp(long x1, long x2) {
    int cmp;
    if ((cmp = (x1 < x2 ? -1 : (x1 == x2 ? 0 : 1))) == 0) return 0;
    // invert the result when either value is negative
    if ((x1 < 0) != (x2 < 0)) return -cmp;
    return cmp;
  }

  /**
   * Write a 32-bit unsigned integer to <code>dst</code> at
   * <code>offset</code> as 4 big-endian bytes.
   * @return number of bytes written.
   */
  public static int putUint32(byte[] dst, int offset, int val) {
    dst[offset] = (byte) ((val >>> 24) & 0xff);
    dst[offset + 1] = (byte) ((val >>> 16) & 0xff);
    dst[offset + 2] = (byte) ((val >>> 8) & 0xff);
    dst[offset + 3] = (byte) (val & 0xff);
    return 4;
  }

  /**
   * @see http://sqlite.org/src4/doc/trunk/www/varint.wiki
   * @see http://www.sqlite.org/src4/finfo?name=src/varint.c,
   *      int sqlite4PutVarint64(unsigned char *z, sqlite4_uint64 x)
   */
  public static int putVaruint64(byte[] dst, int offset, long val) {
    int w, y;
    if (-1 == unsignedCmp(val, 241L)) {
      dst[offset] = (byte) val;
      assert dst[offset] == (byte) (val & 0xff);
      return 1;
    }
    if (-1 == unsignedCmp(val, 2288L)) {
      y = (int) (val - 240);
      dst[offset] = (byte) (y / 256 + 241);
      dst[offset + 1] = (byte) (y % 256);
      return 2;
    }
    if (-1 == unsignedCmp(val, 67824L)) {
      y = (int) (val - 2288);
      dst[offset] = (byte) 249;
      dst[offset + 1] = (byte) (y / 256);
      dst[offset + 2] = (byte) (y % 256);
      return 3;
    }
    y = (int) (val & 0xffffffff);
    w = (int) (val >>> 32);
    if (w == 0) {
      if (-1 == unsignedCmp(y, 16777216L)) {
        dst[offset] = (byte) 250;
        dst[offset + 1] = (byte) ((y >>> 16) & 0xff);
        dst[offset + 2] = (byte) ((y >>> 8) & 0xff);
        dst[offset + 3] = (byte) y;
        return 4;
      }
      dst[offset] = (byte) 251;
      putUint32(dst, offset + 1, y);
      return 5;
    }
    if (-1 == unsignedCmp(w, 256L)) {
      dst[offset] = (byte) 252;
      dst[offset + 1] = (byte) w;
      putUint32(dst, offset + 2, y);
      return 6;
    }
    if (-1 == unsignedCmp(w, 65536L)) {
      dst[offset] = (byte) 253;
      dst[offset + 1] = (byte) ((w >>> 8) & 0xff);
      dst[offset + 2] = (byte) w;
      putUint32(dst, offset + 3, y);
      return 7;
    }
    if (-1 == unsignedCmp(w, 16777216L)) {
      dst[offset] = (byte) 254;
      dst[offset + 1] = (byte) ((w >>> 16) & 0xff);
      dst[offset + 2] = (byte) ((w >>> 8) & 0xff);
      dst[offset + 3] = (byte) w;
      putUint32(dst, offset + 4, y);
      return 8;
    }
    dst[0] = (byte) 255;
    putUint32(dst, offset + 1, w);
    putUint32(dst, offset + 5, y);
    return 9;
  }

  /**
   * Inspect an encoded varu64 for it's encoded length.
   * @param src source buffer
   * @param offset offset into <code>src</code>
   * @return number of bytes consumed by this value
   * @see http://sqlite.org/src4/doc/trunk/www/varint.wiki
   */
  public static int lengthVaru64(byte[] src, int offset) {
    int a0 = src[offset] & 0xff;
    if (a0 <= 240) return 1;
    if (a0 >= 241 && a0 <= 248) return 2;
    if (a0 == 249) return 3;
    if (a0 == 250) return 4;
    if (a0 == 251) return 5;
    if (a0 == 252) return 6;
    if (a0 == 253) return 7;
    if (a0 == 254) return 8;
    if (a0 == 255) return 9;
    throw new IllegalArgumentException("unexpected value a0: " + Long.toHexString(src[offset]));
  }

  /**
   * @see http://sqlite.org/src4/doc/trunk/www/varint.wiki
   * @see http://www.sqlite.org/src4/finfo?name=src/varint.c,
   *      int sqlite4GetVarint64(const unsigned char *z, int n, sqlite4_uint64 *pResult)
   */
  public static long getVaruint64(byte[] src, int offset) {
    assert src.length - offset >= lengthVaru64(src, offset);
    long ret;
    int a0 = src[offset] & 0xff, a1, a2, a3, a4, a5, a6, a7, a8;
    if (-1 == unsignedCmp(a0, 241)) {
      return a0;
    }
    a1 = src[offset + 1] & 0xff;
    if (-1 == unsignedCmp(a0, 249)) {
      return (a0 - 241) * 256 + a1 + 240;
    }
    a2 = src[offset + 2] & 0xff;
    if (a0 == 249) {
      return 2288 + 256 * a1 + a2;
    }
    a3 = src[offset + 3] & 0xff;
    if (a0 == 250) {
      return (a1 << 16) | (a2 << 8) | a3;
    }
    a4 = src[offset + 4] & 0xff;
    // seed ret with unshifted a1 because sign-extension bites us when casting (long) (a1 << 24).
    ret = a1;
    ret = (ret << 24) | ((a2 & 0xff) << 16) | ((a3 & 0xff) << 8) | (a4 & 0xff);
    if (a0 == 251) {
      return ret;
    }
    a5 = src[offset + 5] & 0xff;
    if (a0 == 252) {
      return (ret << 8) | a5;
    }
    a6 = src[offset + 6] & 0xff;
    if (a0 == 253) {
      return (ret << 16) | (a5 << 8) | a6;
    }
    a7 = src[offset + 7] & 0xff;
    if (a0 == 254) {
      return (ret << 24) | (a5 << 16) | (a6 << 8) | a7;
    }
    a8 = src[offset + 8] & 0xff;
    return (ret << 32) | (0xffffffff & ((a5 << 24) | (a6 << 16) | (a7 << 8) | a8));
  }

  public abstract boolean supportsNull();
  public abstract byte[] toBytes(T val);
  public abstract T fromBytes(byte[] bytes);
  public abstract void write(ByteBuffer buff, T val);
  public abstract T read(ByteBuffer buff);
}
