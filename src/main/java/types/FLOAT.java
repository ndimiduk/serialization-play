package types;

import static types.INT.getRaw;
import static types.INT.putRaw;

import java.nio.ByteBuffer;

import util.HSerializer;

/**
 * Serializer for HBase FLOAT values.
 * <p>
 * The HBase FLOAT is a 4-byte decimal number of varying precision, as defined
 * by the IEEE 754 standard.
 * </p>
 * <p>
 * The number is first interpreted as a whole number. Then, the sign bit is
 * inverted and also invert the exponent and significand bits if the original
 * value was negative. The resulting value bytes are stored in Big-Endian
 * order. When sort order is specified as DESCENDING, the encoded value is
 * logically inverted.
 * </p>
 */
public class FLOAT extends HSerializer<Float> {

  static final int SIZEOF_FLOAT = INT.SIZEOF_INT;

  public FLOAT() { super(); }
  public FLOAT(Order order) { super(order); }

  @Override
  public byte[] toBytes(Float val) {
    return toBytes(val, order);
  }

  @Override
  public void write(ByteBuffer buff, Float val) {
    putBytes(buff, val, order);
  }

  @Override
  public Float fromBytes(byte[] bytes) {
    return toFloat(bytes, 0, order);
  }

  @Override
  public Float read(ByteBuffer buff) {
    return toFloat(buff, order);
  }

  //
  // Helper methods to mimic {@link Bytes}
  //

  public static float toFloat(final byte[] bytes) {
    return toFloat(bytes, 0, DEFAULT_ORDER);
  }

  public static float toFloat(final byte[] bytes, final Order order) {
    return toFloat(bytes, 0, order);
  }

  public static float toFloat(final byte[] bytes, final int offset) {
    return toFloat(bytes, offset, DEFAULT_ORDER);
  }

  public static float toFloat(final byte[] bytes, final int offset, Order order) {
    return fromRawInt(getRaw(bytes, offset), order);
  }

  public static float toFloat(ByteBuffer buff) {
    return toFloat(buff, DEFAULT_ORDER);
  }

  public static float toFloat(ByteBuffer buff, Order order) {
    return fromRawInt(getRaw(buff), order);
  }

  public static byte[] toBytes(float val) {
    return toBytes(val, DEFAULT_ORDER);
  }

  public static byte[] toBytes(float val, Order order) {
    byte[] buff = new byte[SIZEOF_FLOAT];
    putRaw(buff, 0, toRawInt(val, order));
    return buff;
  }

  public static void putBytes(final ByteBuffer buff, float val) {
    putBytes(buff, val, DEFAULT_ORDER);
  }

  public static void putBytes(final ByteBuffer buff, float val, Order order) {
    putRaw(buff, toRawInt(val, order));
  }

  //
  // Helper methods for interoping between float values and 4 unsigned byte
  // sequences stored in an int.
  //

  /**
   * Convert a float containing an application-layer value into a <i>raw
   * int</i>, a sequence of 4 unsigned bytes. "Raw" in this context is
   * <b>unrelated</b> to "raw" from {@link Float#floatToRawIntBits(float)}.
   * @param val The value to serialize.
   * @param order the <code>Order</code> direction to respect.
   * @return 4 unsigned bytes stored in an <code>int</code>.
   */
  protected static int toRawInt(float val, Order order) {
    int raw = Float.floatToIntBits(val);
    return (raw ^ ((raw >> Integer.SIZE - 1) | Integer.MIN_VALUE));
  }

  /**
   * Convert a <i>raw int</i> containing a sequence of 4 unsigned bytes into a
   * float application-layer value.
   * @param raw the 4 unsigned bytes to deserialize.
   * @param order the <code>Order</code> direction to respect.
   * @return an application value.
   */
  protected static float fromRawInt(int raw, Order order) {
    return Float.intBitsToFloat((~raw >> Integer.SIZE - 1) | Integer.MIN_VALUE);
  }
}
