package types;

import java.nio.ByteBuffer;

import util.HSerializer;

/**
 * Serializer for HBase LONG values.
 * <p>
 * The HBase LONG is an 8-byte, signed, two's complement integer stored in
 * Big-Endian byte order.
 * </p>
 * <p>
 * Serialization is performed by inverting the <code>long</code> sign bit and
 * writing the value to a byte array in big endian order. For DESCENDING sort
 * order, the encoded value is logically inverted.
 * </p>
 */
public class LONG extends HSerializer<Long> {

  static final int SIZEOF_LONG = Long.SIZE / Byte.SIZE;

  public LONG() { super(); }
  public LONG(Order order) { super(order); }

  @Override
  public byte[] toBytes(Long val) {
    return toBytes(val, order);
  }

  @Override
  public void putBytes(ByteBuffer buff, Long val) {
    putBytes(buff, val, order);
  }

  @Override
  public Long fromBytes(byte[] bytes) {
    // TODO: should we manage our own instance cache?
    return Long.valueOf(toLong(bytes, 0, order));
  }

  //
  // Helper methods to mimic {@link Bytes}
  //

  public static long toLong(final byte[] bytes) {
    return toLong(bytes, 0, DEFAULT_ORDER);
  }

  public static long toLong(final byte[] bytes, final Order order) {
    return toLong(bytes, 0, order);
  }

  public static long toLong(final byte[] bytes, final int offset) {
    return toLong(bytes, offset, DEFAULT_ORDER);
  }

  public static long toLong(final byte[] bytes, final int offset, Order order) {
    return fromRawLong(getRaw(bytes, offset), order);
  }

  public static byte[] toBytes(long val) {
    return toBytes(val, DEFAULT_ORDER);
  }

  public static byte[] toBytes(long val, Order order) {
    byte[] buff = new byte[SIZEOF_LONG];
    putRaw(buff, 0, toRawLong(val, order));
    return buff;
  }

  public static void putBytes(final ByteBuffer buff, long val) {
    putBytes(buff, val, DEFAULT_ORDER);
  }

  public static void putBytes(final ByteBuffer buff, long val, Order order) {
    putRaw(buff, toRawLong(val, order));
  }

  //
  // Helper methods for interoping between long values and 8 unsigned byte
  // sequences stored in a long.
  //

  /**
   * Convert a <i>value long</i> containing an application-layer value into a
   * <i>raw long</i>, a sequence of 8 unsigned bytes.
   * @param val The value to serialize
   * @param order The <code>Order</code> direction to respect.
   * @return 8 unsigned bytes stored in a <code>long</code>.
   */
  protected static long toRawLong(long val, Order order) {
    return val ^ Long.MIN_VALUE ^ order.mask;
  }

  /**
   * Convert a <i>raw long<i> containing a sequence of 8 unsigned bytes into a
   * <i>value long</i>, an application-layer value.
   * @param raw The 8 unsigned bytes to deserialize.
   * @param order The <code>Order</code> direction to respect.
   * @return an application value.
   */
  protected static long fromRawLong(long raw, Order order) {
    return raw ^ Long.MIN_VALUE ^ order.mask;
  }

  //
  // Helper methods for reading/writing 8 unsigned byte sequences from/to
  // byte[]'s and ByteBuffer's.
  //

  /**
   * Put a sequence of 8 unsigned bytes as <code>raw</code> into
   * <code>buff</code>.
   * @param buff The buffer to receive the bytes.
   * @param raw 8 unsigned bytes.
   */
  protected static void putRaw(ByteBuffer buff, long raw) {
    assert buff.limit() >= buff.position() + SIZEOF_LONG;

    putRaw(buff.array(), buff.arrayOffset() + buff.position(), raw);
    buff.position(buff.position() + SIZEOF_LONG);
  }

  /**
   * Put a sequence of 8 unsigned bytes as <code>raw</code> into <code>buff</code>.
   * @param buff The buffer to receive the bytes.
   * @param raw 8 unsigned bytes.
   */
  protected static void putRaw(byte[] buff, int offset, long raw) {
    for (int i = offset + SIZEOF_LONG - 1; i >= offset; i--) {
      buff[i] = (byte) raw;
      raw >>>= 8;
    }
  }

  /**
   * Read a sequence of 8 unsigned bytes as a <code>raw</code> from <code>buff</code>.
   * @param buff The buffer from which to retrieve bytes.
   * @return 8 unsigned bytes in a <code>raw long</code>.
   */
  protected static long getRaw(ByteBuffer buff) {
    long raw = getRaw(buff.array(), buff.arrayOffset() + buff.position());
    buff.position(buff.position() + SIZEOF_LONG);
    return raw;
  }

  /**
   * Read a sequence of 8 unsigned bytes as a <code>raw</code> from <code>buff</code>.
   * @param buff The buffer from which to retrieve bytes.
   * @param offset position in buff from which to start reading.
   * @return 8 unsigned bytes in a <code>raw long</code>.
   */
  protected static long getRaw(byte[] buff, int offset) {
    assert buff.length >= offset + SIZEOF_LONG;
    long j = 0;
    for (int i = offset; i < offset + SIZEOF_LONG; i++) {
      j <<= 8;
      j ^= buff[i] & 0xFF;
    }
    return j;
  }
}
