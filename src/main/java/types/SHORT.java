package types;


import java.nio.ByteBuffer;

import util.HSerializer;

public class SHORT extends HSerializer<Short> {

  static final int SIZEOF_SHORT = Short.SIZE / Byte.SIZE;

  public SHORT() { super(); }
  public SHORT(Order order) { super(order); }

  @Override
  public boolean supportsNull() { return false; }

  @Override
  public byte[] toBytes(Short val) {
    return toBytes(val, order);
  }

  @Override
  public void write(ByteBuffer buff, Short val) {
    putBytes(buff, val, order);
  }

  @Override
  public Short fromBytes(byte[] bytes) {
    // TODO: should we manage our own instance cache?
    return Short.valueOf(toShort(bytes, 0, order));
  }

  @Override
  public Short read(ByteBuffer buff) {
    // TODO: should we manage our own instance cache?
    return Short.valueOf(toShort(buff, order));
  }

  //
  // Helper methods to mimic {@link Bytes}
  //

  public static short toShort(final byte[] bytes) {
    return toShort(bytes, 0, DEFAULT_ORDER);
  }

  public static short toShort(final byte[] bytes, final Order order) {
    return toShort(bytes, 0, order);
  }

  public static short toShort(final byte[] bytes, final int offset) {
    return toShort(bytes, offset, DEFAULT_ORDER);
  }

  public static short toShort(final byte[] bytes, final int offset, Order order) {
    return fromRawShort(getRaw(bytes, offset), order);
  }

  public static short toShort(ByteBuffer buff) {
    return toShort(buff, DEFAULT_ORDER);
  }

  public static short toShort(ByteBuffer buff, Order order) {
    return fromRawShort(getRaw(buff), order);
  }

  public static byte[] toBytes(short val) {
    return toBytes(val, DEFAULT_ORDER);
  }

  public static byte[] toBytes(short val, Order order) {
    byte[] buff = new byte[SIZEOF_SHORT];
    putRaw(buff, 0, toRawShort(val, order));
    return buff;
  }

  public static void putBytes(final ByteBuffer buff, short val) {
    putBytes(buff, val, DEFAULT_ORDER);
  }

  public static void putBytes(final ByteBuffer buff, short val, Order order) {
    putRaw(buff, toRawShort(val, order));
  }

  //
  // Helper methods for interoping between short values and 2 unsigned byte
  // sequences stored in a short.
  //

  /**
   * Convert a <i>value short</i> containing an application-layer value into a
   * <i>raw short</i>, a sequence of 2 unsigned bytes.
   * @param val The value to serialize
   * @param order The <code>Order</code> direction to respect.
   * @return 2 unsigned bytes stored in a <code>short</code>.
   */
  protected static short toRawShort(short val, Order order) {
    return (short) (((int) val) ^ Integer.MIN_VALUE ^ order.mask());
  }

  /**
   * Convert a <i>raw short<i> containing a sequence of 2 unsigned bytes into
   * a <i>value short</i>, an application-layer value.
   * @param raw The 2 unsigned bytes to deserialize.
   * @param order The <code>Order</code> direction to respect.
   * @return an application value.
   */
  protected static short fromRawShort(short raw, Order order) {
    return (short) (((int) raw) ^ Integer.MIN_VALUE ^ order.mask());
  }

  //
  // Helper methods for reading/writing 2 unsigned byte sequences from/to
  // byte[]'s and ByteBuffer's.
  //

  /**
   * Put a sequence of 2 unsigned bytes as <code>raw</code> into <code>buff</code>.
   * @param buff The buffer to receive the bytes.
   * @param raw 2 unsigned bytes.
   */
  protected static void putRaw(ByteBuffer buff, short raw) {
    assert buff.limit() >= buff.position() + SIZEOF_SHORT;

    putRaw(buff.array(), buff.arrayOffset() + buff.position(), raw);
    buff.position(buff.position() + SIZEOF_SHORT);
  }

  /**
   * Put a sequence of 2 unsigned bytes as <code>raw</code> into <code>buff</code>.
   * @param buff The buffer to receive the bytes.
   * @param raw 2 unsigned bytes.
   */
  protected static void putRaw(byte[] buff, int offset, short raw) {
    for (int i = offset + SIZEOF_SHORT - 1; i >= offset; i--) {
      buff[i] = (byte) raw;
      raw >>>= 8;
    }
  }

  /**
   * Read a sequence of 2 unsigned bytes as a <code>raw</code> from <code>buff</code>.
   * @param buff The buffer from which to retrieve bytes.
   * @return 2 unsigned bytes in a <code>raw short</code>.
   */
  protected static short getRaw(ByteBuffer buff) {
    short raw = getRaw(buff.array(), buff.arrayOffset() + buff.position());
    buff.position(buff.position() + SIZEOF_SHORT);
    return raw;
  }

  /**
   * Read a sequence of 2 unsigned bytes as a <code>raw</code> from <code>buff</code>.
   * @param buff The buffer from which to retrieve bytes.
   * @param offset position in buff from which to start reading.
   * @return 2 unsigned bytes in a <code>raw short</code>.
   */
  protected static short getRaw(byte[] buff, int offset) {
    assert buff.length >= offset + SIZEOF_SHORT;
    short j = 0;
    for (int i = offset; i < offset + SIZEOF_SHORT; i++) {
      j <<= 8;
      j ^= buff[i] & 0xFF;
    }
    return j;
  }
}
