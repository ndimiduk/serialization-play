package types;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.util.Bytes;

import util.HSerializer;

/**
 * Serializer for HBase BOOLEAN values.
 * <p>
 * The HBase BOOLEAN is an 1-byte value stored in Big-Endian byte order.
 * </p>
 * <p>
 * A True is represented by all 1-bits and False by all 0-bits. For DESCENDING
 * sort order, the encoded value is logically inverted.
 * </p>
 */
public class BOOLEAN extends HSerializer<Boolean> {

  static final int SIZEOF_BOOLEAN = Byte.SIZE / Byte.SIZE;
  static final byte TRUE  = (byte) 0xFF;
  static final byte FALSE = (byte) 0x01;
  static final byte NULL  = (byte) 0x00;

  public BOOLEAN() { super(); }
  public BOOLEAN(Order order) { super(order); }

  @Override
  public byte[] toBytes(Boolean val) {
    if (null == val) return new byte[] { (byte) (NULL ^ order.mask()) };
    return toBytes(val, order);
  }

  @Override
  public void write(ByteBuffer buff, Boolean val) {
    if (null == val)
      buff.put(new byte[] { (byte) (NULL ^ order.mask()) });
    else
      putBytes(buff, val, order);
  }

  @Override
  public Boolean fromBytes(byte[] bytes) {
    assert bytes.length == 1;
    switch (bytes[0] ^ order.mask()) {
    case NULL:
      return null;
    case FALSE:
      return Boolean.FALSE;
    case TRUE:
      return Boolean.TRUE;
    default:
      throw new IllegalArgumentException("Unexpected byte value " + toBinaryString(bytes));
    }
  }

  @Override
  public Boolean read(ByteBuffer buff) {
    byte b = buff.get();
    switch (b ^ order.mask()) {
    case NULL:
      return null;
    case FALSE:
      return Boolean.FALSE;
    case TRUE:
      return Boolean.TRUE;
    default:
      throw new IllegalArgumentException("Unexpected byte value " + toBinaryString(b));
    }
  }

  //
  // Helper methods to mimic {@link Bytes}
  //

  public static boolean toBoolean(byte[] bytes) {
    return toBoolean(bytes, DEFAULT_ORDER);
  }

  public static boolean toBoolean(byte[] bytes, Order order) {
    assert bytes.length == 1;
    switch (bytes[0] ^ order.mask()) {
    case NULL:
      throw new IllegalArgumentException("primitive interface does not support NULL values.");
    case FALSE:
      return false;
    case TRUE:
      return true;
    default:
      throw new IllegalArgumentException("Unexpected byte value " + toBinaryString(bytes));
    }
  }

  public static byte[] toBytes(final boolean val) {
    return toBytes(val, DEFAULT_ORDER);
  }

  public static byte[] toBytes(final boolean val, final Order order) {
    return new byte[] { (byte) ((val ? TRUE : FALSE) ^ order.mask()) };
  }

  public static void putBytes(final ByteBuffer buff, final boolean val) {
    putBytes(buff, val, DEFAULT_ORDER);
  }

  public static void putBytes(final ByteBuffer buff, final boolean val, final Order order) {
    Bytes.putByte(buff.array(), buff.arrayOffset() + buff.position(),
      (byte) ((val ? TRUE : FALSE) ^ order.mask()));
    buff.position(buff.position() + SIZEOF_BOOLEAN);
  }
}
