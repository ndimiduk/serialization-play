package util;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
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

  private static int unsignedCmp(long x1, long x2) {
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
  private static int putUint32(byte[] dst, int offset, int val) {
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
   * Return the number of bytes needed to represent a 64-bit signed integer.
   * @see http://www.sqlite.org/src4/finfo?name=src/vdbecodec.c,
   *      static int significantBytes(sqlite4_int64 v)
   */
  private static int significantBytes(long v) {
    long x;
    int n = 1;
    if (v > 0) {
      x = -128;
      while (v < x && n < 8) { n++; x *= 256; }
    } else {
      x = 127;
      while (v > x && n < 8) { n++; x *= 256; }
    }
    return n;
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

  /**
   * Encode the positive integer m using the key encoding.
   * @return E(xponent)
   *
   * @see http://sqlite.org/src4/doc/trunk/www/key_encoding.wiki
   * @see http://www.sqlite.org/src4/finfo?name=src/vdbecodec.c,
   *      static int encodeIntKey(sqlite4_uint64 m, KeyEncoder *p)
   */
  private static int _encodeIntKey(ByteBuffer buff, long m) {
    assert m > 0;
    int i = 0, e;
    byte[] digits = new byte[20];
    do {
      digits[i++] = (byte) ((m % 100) & 0xff);
      m /= 100;
    } while (m > 0);
    e = i;
    assert e >= 1 && e <= 10;
    while (i > 0)
      buff.put((byte) ((digits[--i] * 2 + 1) & 0xff));
    buff.array()[buff.position()] &= 0xfe;
    return e;
  }

  /**
   * Encode a single integer using the key encoding. The caller must ensure
   * that sufficient space exits in a[] (at least 12 bytes). The return value
   * is the number of bytes of a[] used.
   *
   * @see http://sqlite.org/src4/doc/trunk/www/key_encoding.wiki
   * @see http://www.sqlite.org/src4/finfo?name=src/vdbecodec.c,
   *      int sqlite4VdbeEncodeIntKey(u8 *a, sqlite4_int64 v)
   */
  public static void encodeIntKey(ByteBuffer buff, long v) {
    int e;
    if (v < 0) {
      // TODO: use mark() instead of startPos?
      int startPos = buff.position();
      buff.position(buff.position() + 1);
      e = _encodeIntKey(buff, -v);
      assert e <= 10;
      // "finite negative values will have initial bytes of 0x08 through 0x14"
      buff.put(startPos, (byte) ((0x13 - e) & 0xff));
      for (int i = startPos + 1; i < buff.position() + 1; i++)
        buff.array()[i] ^= 0xff;
      return;
    }
    if (v > 0) {
      // "Medium positive values are a single byte of 0x17+E followed by M"
      int startPos = buff.position();
      buff.position(buff.position() + 1);
      e = _encodeIntKey(buff, v);
      assert e <= 10;
      // "finite positive values will have initial bytes of 0x16 through 0x22"
      buff.put(startPos, (byte) ((0x17 + e) & 0xff));
      return;
    }
    buff.put((byte) 0x15);
  }

  /**
   * Encode the small positive floating point number r using the key encoding.
   * The caller guarantees that r will be less than 1.0 and greater than 0.0.
   *
   * @see http://sqlite.org/src4/doc/trunk/www/key_encoding.wiki
   * @see http://www.sqlite.org/src4/finfo?name=src/vdbecodec.c,
   *      static void encodeSmallFloatKey(double r, KeyEncoder *p)
   */
  private static void encodeSmallFloatKey(ByteBuffer buff, double r) {
    assert r > 0.0 && r < 1.0;
    int e = 0, n, d;
    while (r < 1e-10) { r *= 1e8; e += 4; }
    while (r < 0.01) { r *= 100.0; e++; }
    n = putVaruint64(buff.array(), buff.position(), e);
    for (int i = buff.position(); i < buff.position() + n; i++)
      buff.array()[i] ^= 0xff;
    buff.position(buff.position() + n);
    for (int i = 0; i < 18 && r != 0.0; i++) {
      r *= 100.0;
      d = (int) r;
      buff.put((byte) ((2 * d + 1) & 0xff));
      r -= d;
    }
    buff.array()[buff.position()] &= 0xfe;
  }

  /**
   * Encode the large positive floating point number r using the key encoding.
   * The caller guarantees that r will be finite and greater than or equal to
   * 1.0.
   * @return E(xponent)
   *
   * @see http://sqlite.org/src4/doc/trunk/www/key_encoding.wiki
   * @see http://www.sqlite.org/src4/finfo?name=src/vdbecodec.c,
   *      static int encodeLargeFloatKey(double r, KeyEncoder *p)
   */
  private static int encodeLargeFloatKey(ByteBuffer buff, double r) {
    assert r >= 1.0;
    int e = 0, n, d;
    while (r >= 1e32 && e <= 350) { r *= 1e-32; e +=16; }
    while (r >= 1e8 && e <= 350) { r *= 1e-8; e+= 4; }
    while (r >= 1.0 && e <= 350) { r *= 0.01; e++; }
    if (e > 10) {
      n = putVaruint64(buff.array(), buff.position(), e);
      buff.position(buff.position() + n);
    }
    for (int i = 0; i < 18 && r != 0.0; i++) {
      r *= 100.0;
      d = (int) r;
      buff.put((byte) ((2 * d + 1) & 0xff));
      r -= d;
    }
    buff.array()[buff.position()] &= 0xfe;
    return e;
  }

  /**
   * Encode a null value
   *
   * @see http://sqlite.org/src4/doc/trunk/www/key_encoding.wiki
   * @see http://www.sqlite.org/src4/finfo?name=src/vdbecodec.c,
   *      static int encodeOneKeyValue(...)
   */
  private static void encodeNull(ByteBuffer buff) {
    buff.put((byte) 0x05);
  }

  /**
   * Encode an integral value
   *
   * @see http://sqlite.org/src4/doc/trunk/www/key_encoding.wiki
   * @see http://www.sqlite.org/src4/finfo?name=src/vdbecodec.c,
   *      static int encodeOneKeyValue(...)
   */
  private static void encodeInt(ByteBuffer buff, long v) {
    int e, i;
    if (v == 0) {
      buff.put((byte) 0x15); /* Numeric zero */
    } else if (v < 0) {
      buff.put((byte) 0x08); /* Large negative number */
      i = buff.position();
      e = _encodeIntKey(buff, -v);
      if (e <= 10) buff.put((byte) (0x13 - e)); /* Medium negative number */
      while (i <= buff.position()) buff.array()[i++] ^= 0xff;
    } else {
      buff.put((byte) 0x22); /* Large positive number */
      i = buff.position();
      e = _encodeIntKey(buff, v);
      if (e <= 10) buff.put(i, (byte) (0x17 + e)); /* Medium positive number */
    }
  }

  /**
   * Encode a Real value
   *
   * @see http://sqlite.org/src4/doc/trunk/www/key_encoding.wiki
   * @see http://www.sqlite.org/src4/finfo?name=src/vdbecodec.c,
   *      static int encodeOneKeyValue(...)
   */
  private static void encodeReal(ByteBuffer buff, double r) {
    int e, i;
    byte[] a;
    if (r == 0.0) {
      buff.put((byte) 0x15); /* Numeric zero */
    } else if (Double.isNaN(r)) {
      buff.put((byte) 0x06); /* NaN */
    } else if (Double.NEGATIVE_INFINITY == r) {
      buff.put((byte) 0x07);
    } else if (Double.POSITIVE_INFINITY == r) {
      buff.put((byte) 0x23);
    } else if (r <= -1.0) {
      buff.put((byte) 0x08); /* Large negative values */
      i = buff.position();
      e = encodeLargeFloatKey(buff, -r);
      if (e <= 10) buff.put(i, (byte) (0x13 - e)); /* Medium negative values */
      a = buff.array();
      while (i <= buff.position()) a[i++] ^= 0xff;
    } else if (r < 0.0) {
      buff.put((byte) 0x14); /* Small negative values */
      i = buff.position();
      encodeSmallFloatKey(buff, -r);
      a = buff.array();
      while (i <= buff.position()) a[i++] ^= 0xff;
    } else if (r < 1.0) {
      buff.put((byte) 0x16); /* Small positive values */
      encodeSmallFloatKey(buff, r);
    } else {
      buff.put((byte) 0x22); /* Large positive values */
      i = buff.position();
      e = encodeLargeFloatKey(buff, r);
      if (e <= 10) buff.put(i, (byte) (0x17 + e)); /* Medium positive values. */
    }
  }

  /**
   * Encode a Text value
   *
   * @see http://sqlite.org/src4/doc/trunk/www/key_encoding.wiki
   * @see http://www.sqlite.org/src4/finfo?name=src/vdbecodec.c,
   *      static int encodeOneKeyValue(...)
   */
  private static void encodeString(ByteBuffer buff, String s) {
    buff.put((byte) 0x24);
    buff.put(s.getBytes(Charset.forName("UTF-8")));
    buff.put((byte) 0x00);
  }

  /**
   * Encode a Blob value, last element in Key
   *
   * @see http://sqlite.org/src4/doc/trunk/www/key_encoding.wiki
   * @see http://www.sqlite.org/src4/finfo?name=src/vdbecodec.c,
   *      static int encodeOneKeyValue(...)
   */
  private static void encodeBlobLast(ByteBuffer buff, byte[] b) {
    // Blobs as final entry in a compound key are written unencoded.
    assert buff.remaining() >= b.length + 1;
    buff.put((byte) 0x26);
    buff.put(b);
  }

  /**
   * Encode a Blob value, intermediate element in Key
   *
   * @see http://sqlite.org/src4/doc/trunk/www/key_encoding.wiki
   * @see http://www.sqlite.org/src4/finfo?name=src/vdbecodec.c,
   *      static int encodeOneKeyValue(...)
   */
  private static void encodeBlobMid(ByteBuffer buff, byte[] b) {
    // Blobs as intermediate entries are encoded as 7-bits per byte, null-terminated.
    assert buff.remaining() >= (b.length * 8 + 6) / 7 + 2;
    buff.put((byte) 0x26); /* Blob */
    byte s = 1, t = 0;
    for (int i = 0; i < b.length; i++) {
      buff.put((byte) (0x80 | t | (b[i] >>> s)));
      if (s < 7) {
        t = (byte) (b[i] << (7 - s));
        s++;
      } else {
        buff.put((byte) (0x80 | b[i]));
        s = 1;
        t = 0;
      }
    }
    if (s > 1) buff.put((byte) (0x80 | t));
    buff.put((byte) 0x00);
  }

  /**
   * Skip <code>buff.position()</code> forward <code>n</code> entries. This is
   *
   * @see http://sqlite.org/src4/doc/trunk/www/key_encoding.wiki
   * @see http://www.sqlite.org/src4/finfo?name=src/vdbecodec.c,
   *      int sqlite4VdbeShortKey(const u8 *aKey, int nKey, int nField)
   */
  public static void seekn(ByteBuffer buff, int n) {
    // TODO
  }

  /**
   * Encode a sequence of values into a compound key.
   *
   * @see http://sqlite.org/src4/doc/trunk/www/key_encoding.wiki
   * @see http://www.sqlite.org/src4/finfo?name=src/vdbecodec.c,
   *      int sqlite4VdbeEncodeKey(...)
   */
  public static void encode(ByteBuffer buff, Object[] vals) {
    for (int i = 0; i < vals.length; i++) {
      if (null == vals[i]) {
        encodeNull(buff);
        continue;
      }
      Class<?> c = vals[i].getClass();
      if (Boolean.class.isAssignableFrom(c) || Character.class.isAssignableFrom(c)
          || Byte.class.isAssignableFrom(c) || Short.class.isAssignableFrom(c)
          || Integer.class.isAssignableFrom(c) || Long.class.isAssignableFrom(c)) {
        encodeInt(buff, (Long) vals[i]);
        continue;
      }
      if (Float.class.isAssignableFrom(c) || Double.class.isAssignableFrom(c)) {
        encodeReal(buff, (Double) vals[i]);
        continue;
      }
      if (String.class.isAssignableFrom(c)) {
        encodeString(buff, (String) vals[i]);
        continue;
      }
      if (byte[].class.isAssignableFrom(c)) {
        if (i == vals.length - 1)
          encodeBlobLast(buff, (byte[]) vals[i]);
        else
          encodeBlobMid(buff, (byte[]) vals[i]);
        continue;
      }
      assert false : "No registered handler for Object of type " + vals[i].getClass().getSimpleName();
    }
  }

  public abstract boolean supportsNull();
  public abstract byte[] toBytes(T val);
  public abstract T fromBytes(byte[] bytes);
  public abstract void write(ByteBuffer buff, T val);
  public abstract T read(ByteBuffer buff);
}
