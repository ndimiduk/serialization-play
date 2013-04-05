package types;

import java.nio.ByteBuffer;
import java.util.Date;

import util.HSerializer;

/**
 * Serializer for HBase DATETIME values.
 * <p>
 * The HBase DATETIME represents an instant in time. It is stored as a
 * {@link LONG} value representing milliseconds from the epoch.
 * </p>
 */
public class DATETIME extends HSerializer<Date> {

  static final int SIZEOF_DATETIME = LONG.SIZEOF_LONG;

  public DATETIME() { super(); }
  public DATETIME(Order order) { super(order); }

  @Override
  public byte[] toBytes(Date val) {
    return toBytes(val.getTime(), order);
  }

  @Override
  public void putBytes(ByteBuffer buff, Date val) {
    putBytes(buff, val.getTime(), order);
  }

  @Override
  public Date fromBytes(byte[] bytes) {
    return new Date(LONG.toLong(bytes, 0, order));
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
    return LONG.toLong(bytes, offset, order);
  }

  public static byte[] toBytes(long val) {
    return toBytes(val, DEFAULT_ORDER);
  }

  public static byte[] toBytes(long val, Order order) {
    return LONG.toBytes(val, order);
  }

  public static void putBytes(final ByteBuffer buff, long val) {
    putBytes(buff, val, DEFAULT_ORDER);
  }

  public static void putBytes(final ByteBuffer buff, long val, Order order) {
    LONG.putBytes(buff, val, order);
  }
}
