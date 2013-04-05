package types;

import java.nio.ByteBuffer;

import util.HSerializer;

/**
 * Serializer for HBase INT values.
 * <p>
 * The HBase LONG is an 4-byte, signed, two's complement integer stored in
 * Big-Endian byte order.
 * </p>
 * <p>
 * Serialization is performed by inverting the <code>int</code> sign bit and
 * writing the value to a byte array in big endian order. For DESCENDING sort
 * order, the encoded value is logically inverted.
 * </p>
 */
public class INT extends HSerializer<Integer> {

  static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;

  public INT() { super(); }
  public INT(Order order) { super(order); }

  @Override
  public byte[] toBytes(Integer val) {
    return toBytes(val, order);
  }

  @Override
  public void putBytes(ByteBuffer buff, Integer val) {
    putBytes(buff, val, order);
  }

  @Override
  public Integer fromBytes(byte[] bytes) {
    // TODO: should we manage our own instance cache?
    return Integer.valueOf(toInt(bytes, 0, order));
  }

  //
  // Helper methods to mimic {@link Bytes}
  //

  public static int toInt(final byte[] bytes) {
    return toInt(bytes, 0, DEFAULT_ORDER);
  }

  public static int toInt(final byte[] bytes, final Order order) {
    return toInt(bytes, 0, order);
  }

  public static int toInt(final byte[] bytes, final int offset) {
    return toInt(bytes, offset, DEFAULT_ORDER);
  }

  public static int toInt(final byte[] bytes, final int offset, Order order) {
    return fromRawInt(getRaw(bytes, offset), order);
  }

  public static byte[] toBytes(int val) {
    return toBytes(val, DEFAULT_ORDER);
  }

  public static byte[] toBytes(int val, Order order) {
    byte[] buff = new byte[SIZEOF_INT];
    putRaw(buff, 0, toRawInt(val, order));
    return buff;
  }

  public static void putBytes(final ByteBuffer buff, int val) {
    putBytes(buff, val, DEFAULT_ORDER);
  }

  public static void putBytes(final ByteBuffer buff, int val, Order order) {
    putRaw(buff, toRawInt(val, order));
  }

  //
  // Helper methods for interoping between int values and 4 unsigned byte
  // sequences stored in an int.
  //

  /**
   * Convert a <i>value int</i> containing an application-layer value into a
   * <i>raw int</i>, a sequence of 4 unsigned bytes.
   * @param val The value to serialize
   * @param order The <code>Order</code> direction to respect.
   * @return 4 unsigned bytes stored in an <code>int</code>.
   */
  protected static int toRawInt(int val, Order order) {
    return val ^ Integer.MIN_VALUE ^ order.mask;
  }

  /**
   * Convert a <i>raw int<i> containing a sequence of 4 unsigned bytes into a
   * <i>value int</i>, an application-layer value.
   * @param raw The 4 unsigned bytes to deserialize.
   * @param order The <code>Order</code> direction to respect.
   * @return an application value.
   */
  protected static int fromRawInt(int raw, Order order) {
    return raw ^ Integer.MIN_VALUE ^ order.mask;
  }

  //
  // Helper methods for reading/writing 4 unsigned byte sequences from/to
  // byte[]'s and ByteBuffer's.
  //

  /**
   * Put a sequence of 4 unsigned bytes as <code>raw</code> into
   * <code>buff</code>.
   * @param buff The buffer to receive the bytes.
   * @param raw 4 unsigned bytes.
   */
  protected static void putRaw(ByteBuffer buff, int raw) {
    assert buff.limit() >= buff.position() + SIZEOF_INT;

    putRaw(buff.array(), buff.arrayOffset() + buff.position(), raw);
    buff.position(buff.position() + SIZEOF_INT);
  }

  /**
   * Put a sequence of 4 unsigned bytes as <code>raw</code> into <code>buff</code>.
   * @param buff The buffer to receive the bytes.
   * @param raw 4 unsigned bytes.
   */
  protected static void putRaw(byte[] buff, int offset, int raw) {
    for (int i = offset + SIZEOF_INT - 1; i >= offset; i--) {
      buff[i] = (byte) raw;
      raw >>>= 8;
    }
  }

  /**
   * Read a sequence of 4 unsigned bytes as a <code>raw</code> from <code>buff</code>.
   * @param buff The buffer from which to retrieve bytes.
   * @return 4 unsigned bytes in a <code>raw long</code>.
   */
  protected static int getRaw(ByteBuffer buff) {
    int raw = getRaw(buff.array(), buff.arrayOffset() + buff.position());
    buff.position(buff.position() + SIZEOF_INT);
    return raw;
  }

  /**
   * Read a sequence of 4 unsigned bytes as a <code>raw</code> from <code>buff</code>.
   * @param buff The buffer from which to retrieve bytes.
   * @param offset position in buff from which to start reading.
   * @return 4 unsigned bytes in a <code>raw long</code>.
   */
  protected static int getRaw(byte[] buff, int offset) {
    assert buff.length >= offset + SIZEOF_INT;
    int j = 0;
    for (int i = offset; i < offset + SIZEOF_INT; i++) {
      j <<= 8;
      j ^= buff[i] & 0xFF;
    }
    return j;
  }
}
