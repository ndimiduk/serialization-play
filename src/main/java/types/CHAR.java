package types;

import static java.lang.String.format;

import java.nio.ByteBuffer;

import util.HSerializer;

/**
 * Serializer for HBase CHAR values.
 * <p>
 * The HBase CHAR is an arbitrary number of unicode characters, limited to
 * <code>length</code> bytes when serialized.
 * </p>
 * <p>
 * Serialization is identical to that of {@link VARCHAR}, with the addition of
 * length constraint checking.
 * </p>
 */
public class CHAR extends HSerializer<String> {

  private static final String TOO_LARGE_FMT = "Encoded value does not fit on %d bytes.";
  private final int length;

  public CHAR(int length) {
    super();
    this.length = length;
  }

  public CHAR(int length, Order order) {
    super(order);
    this.length = length;
  }

  /**
   * Ensure <code>val</code> + a termination byte fit within <code>len</code>
   * constraint. This is only a heuristic to detect overly-ambitious input
   * Strings. Definitive checking is handled by
   * {@link #validate(int, byte[])}.
   * @param len maximum length of resulting encoded value.
   * @param val String value to test.
   * @return val, for chaining purposes.
   */
  protected static String validate(int len, String val) {
    if (null == val) return val;
    if (val.length() + 1 <= len) return val;
    throw new IllegalArgumentException(format(TOO_LARGE_FMT, len));
  }

  /**
   * Ensure <code>val</code> fits within <code>len</code> constraint.
   * @param len maximum length of resulting encoded value.
   * @param val String value to test.
   * @param offset start validation after indexing this far into <code>val</code>.
   * @return val, for chaining purposes.
   */
  protected static byte[] validate(int len, byte[] val, int offset) {
    if (val.length - offset <= len) return val;
    throw new IllegalArgumentException(format(TOO_LARGE_FMT, len));
  }

  @Override
  public byte[] toBytes(String val) {
    return validate(length, VARCHAR.toBytes(validate(length, val), order), 0);
  }

  @Override
  public void putBytes(ByteBuffer buff, String val) {
  }

  @Override
  public String fromBytes(byte[] bytes) {
    return VARCHAR.toString(validate(length, bytes, 0), 0, order);
  }

  //
  // Helper methods to mimic {@link Bytes}
  //

  public static byte[] toBytes(int length, String val, Order order) {
    return validate(length, VARCHAR.toBytes(validate(length, val), order), 0);
  }

  public static ByteBuffer putBytes(int length, ByteBuffer buff, String val, Order order) {
    // TODO: reimplement with fewer allocation/copies
    return buff.put(toBytes(length, val, order));
  }

  public static byte[] putBytes(int length, byte[] dst, int dstOffset, String val, Order order) {
    // don't populate the ByteBuffer directly so that it remains unmodified in
    // the event of constraint failure.
    byte[] bytes = validate(length, VARCHAR.toBytes(validate(length, val), order), 0);
    System.arraycopy(bytes, 0, dst, dstOffset, bytes.length);
    return dst;
  }

  public static String toString(int length, byte[] bytes) {
    return VARCHAR.toString(validate(length, bytes, 0), 0, DEFAULT_ORDER);
  }

  public static String toString(int length, byte[] bytes, int offset) {
    return VARCHAR.toString(validate(length, bytes, offset), offset, DEFAULT_ORDER);
  }

  public static String toString(int length, byte[] bytes, int offset, Order order) {
    return VARCHAR.toString(validate(length, bytes, offset), offset, order);
  }
}
