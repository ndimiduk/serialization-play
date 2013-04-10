package types;

import static types.LONG.getRaw;
import static types.LONG.putRaw;

import java.nio.ByteBuffer;

import util.HSerializer;

/**
 * Serializer for HBase DOUBLE values.
 * <p>
 * The HBase DOUBLE is an 8-byte decimal number of varying precision, as
 * defined by the IEEE 754 standard.
 * </p>
 * <p>
 * The number is first interpreted as a whole number. Then, the sign bit is
 * inverted and also invert the exponent and significand bits if the original
 * value was negative. The resulting value bytes are stored in Big-Endian
 * order. When sort order is specified as DESCENDING, the encoded value is
 * logically inverted.
 * </p>
 */
public class DOUBLE extends HSerializer<Double> {

  static final int SIZEOF_DOUBLE = LONG.SIZEOF_LONG;

  public DOUBLE() { super(); }
  public DOUBLE(Order order) { super(order); }

  @Override
  public boolean supportsNull() { return false; }

  @Override
  public byte[] toBytes(Double val) {
    return toBytes(val, order);
  }

  @Override
  public void write(ByteBuffer buff, Double val) {
    putBytes(buff, val, order);
  }

  @Override
  public Double fromBytes(byte[] bytes) {
    return toDouble(bytes, 0, order);
  }

  @Override
  public Double read(ByteBuffer buff) {
    return toDouble(buff, order);
  }

  //
  // Helper methods to mimic {@link Bytes}
  //

  public static double toDouble(final byte[] bytes) {
    return toDouble(bytes, 0, DEFAULT_ORDER);
  }

  public static double toDouble(final byte[] bytes, final Order order) {
    return toDouble(bytes, 0, order);
  }

  public static double toDouble(final byte[] bytes, final int offset) {
    return toDouble(bytes, offset, DEFAULT_ORDER);
  }

  public static double toDouble(final byte[] bytes, final int offset, Order order) {
    return fromRawLong(getRaw(bytes, offset), order);
  }

  public static double toDouble(ByteBuffer buff) {
    return toDouble(buff, DEFAULT_ORDER);
  }

  public static double toDouble(ByteBuffer buff, Order order) {
    return fromRawLong(getRaw(buff), order);
  }

  public static byte[] toBytes(double val) {
    return toBytes(val, DEFAULT_ORDER);
  }

  public static byte[] toBytes(double val, Order order) {
    byte[] buff = new byte[SIZEOF_DOUBLE];
    putRaw(buff, 0, toRawLong(val, order));
    return buff;
  }

  public static void putBytes(final ByteBuffer buff, double val) {
    putBytes(buff, val, DEFAULT_ORDER);
  }

  public static void putBytes(final ByteBuffer buff, double val, Order order) {
    putRaw(buff, toRawLong(val, order));
  }

  //
  // Helper methods for interoping between float values and 4 unsigned byte
  // sequences stored in an int.
  //

  /**
   * Convert a double containing an application-layer value into a <i>raw
   * long</i>, a sequence of 8 unsigned bytes. "Raw" in this context is
   * <b>unrelated</b> to "raw" from {@link Double#doubleToRawLongBits(double)}.
   * @param val The value to serialize.
   * @param order the <code>Order</code> direction to respect.
   * @return 8 unsigned bytes stored in a <code>long</code>.
   */
  protected static long toRawLong(double val, Order order) {
    long raw = Double.doubleToLongBits(val);
    return (raw ^ ((raw >> Long.SIZE - 1) | Long.MIN_VALUE));
  }

  /**
   * Convert a <i>raw long</i> containing a sequence of 8 unsigned bytes into
   * a double application-layer value.
   * @param raw the 8 unsigned bytes to deserialize.
   * @param order the <code>Order</code> direction to respect.
   * @return an application value.
   */
  protected static double fromRawLong(long raw, Order order) {
    return Double.longBitsToDouble((~raw >> Long.SIZE - 1) | Long.MIN_VALUE);
  }
}
