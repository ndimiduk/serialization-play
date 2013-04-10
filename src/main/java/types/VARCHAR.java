package types;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import util.HSerializer;

/**
 * Serializer for HBase VARCHAR values.
 * <p>
 * The HBase VARCHAR is an arbitrary number of unicode characters.
 * </p>
 * <p>
 * Serialization is performed by writing the encoded the String as UTF-8, in
 * big endian byte order, followed by a (null) termination byte. For
 * DESCENDING sort order, the encoded value is logically inverted.
 * </p>
 */
public class VARCHAR extends HSerializer<String> {

  static final Charset UTF8 = Charset.forName("UTF-8");

  static final byte NULL = (byte) 0x00;
  static final byte TERM = (byte) 0x01;

  public VARCHAR() { super(); }
  public VARCHAR(Order order) { super(order); }

  /**
   * Return byte resulting from application of sort-order mask to <code>b</code>.
   */
  protected static byte mask(Order o, byte b) {
    return (byte) (b ^ o.mask());
  }

  @Override
  public byte[] toBytes(String val) {
    return toBytes(val, order);
  }

  @Override
  public void write(ByteBuffer buff, String val) {
    putBytes(buff, val, order);
  }

  @Override
  public String fromBytes(byte[] bytes) {
    return toString(bytes, 0, order);
  }

  @Override
  public String read(ByteBuffer buff) {
    return toString(buff);
  }

  //
  // Helper methods to mimic {@link Bytes}
  //

  // TODO: refactor {to,put}Bytes M-w, C-y, a la LONG raw helpers.

  public static byte[] toBytes(String val, Order order) {
    if (null == val) {
      return new byte[] { mask(order, NULL), mask(order, TERM) };
    }

    // TODO: reimplement with fewer allocations/copies
    byte[] encoded = val.getBytes(UTF8);
    for (int i = 0; i < encoded.length; i++) {
      encoded[i] = mask(order, (byte) (encoded[i] + 2));
    }
    byte[] ret = new byte[encoded.length + 1];
    ret[encoded.length] = mask(order, TERM);
    System.arraycopy(encoded, 0, ret, 0, encoded.length);
    return ret;
  }

  public static byte[] putBytes(byte[] dst, int dstOffset, String val, Order order) {
    if (null == val) {
      assert dst.length >= dstOffset + 2;
      dst[dstOffset] = mask(order, NULL);
      dst[dstOffset + 1] = mask(order, TERM);
      return dst;
    }

    // TODO: reimplement with fewer allocations/copies
    byte[] encoded = val.getBytes(UTF8);
    for (int i = 0; i < encoded.length; i++) {
      encoded[i] = mask(order, (byte) (encoded[i] + 2));
    }
    assert dst.length >= dstOffset + encoded.length + 1;
    dst[dstOffset + encoded.length] = mask(order, TERM);
    System.arraycopy(encoded, 0, dst, dstOffset, encoded.length);
    return dst;
  }

  public static ByteBuffer putBytes(ByteBuffer buff, String val, Order order) {
    if (null == val) {
      assert buff.limit() >= buff.position() + 2;
      buff.put(mask(order, NULL));
      return buff.put(mask(order, TERM));
    }

    // TODO: reimplement with fewer allocations/copies
    byte[] encoded = val.getBytes(UTF8);
    for (int i = 0; i < encoded.length; i++) {
      encoded[i] = mask(order, (byte) (encoded[i] + 2));
    }
    assert buff.limit() >= buff.position() + encoded.length + 1;
    System.arraycopy(encoded, 0, buff.array(), buff.position(), encoded.length);
    buff.position(buff.position() + encoded.length);
    return buff.put(mask(order, TERM));
  }

  public static String toString(byte[] bytes) {
    return toString(bytes, 0, DEFAULT_ORDER);
  }

  public static String toString(byte[] bytes, int offset) {
    return toString(bytes, offset, DEFAULT_ORDER);
  }

  public static String toString(byte[] bytes, int offset, Order order) {
    if (mask(order, bytes[offset]) == TERM) return "";
    if (mask(order, bytes[offset]) == NULL && mask(order, bytes[offset + 1]) == TERM)
      return null;

    ByteBuffer decoded = ByteBuffer.allocate(bytes.length - offset - 1);
    for (int i = offset; i < bytes.length; i++) {
      if (TERM == mask(order, bytes[i]))
        break;
      decoded.put((byte) (mask(order, bytes[i]) - 2));
    }
    byte[] ret = new byte[decoded.position()];
    System.arraycopy(decoded.array(), 0, ret, 0, ret.length);
    return new String(ret, UTF8);
  }

  public static String toString(ByteBuffer buff) {
    return toString(buff, DEFAULT_ORDER);
  }

  public static String toString(ByteBuffer buff, Order order) {
    // locate the terminal byte
    int initalPosition = buff.position();
    while (mask(order, buff.get()) != TERM);
    if (initalPosition == buff.position() - 1) return "";
    if (initalPosition == buff.position() - 2 && mask(order, buff.get(initalPosition)) == TERM)
      return null;

    byte[] decoded = new byte[buff.position() - initalPosition - 1];
    for (int i = initalPosition; i < buff.position() - 1; i++) {
      decoded[i - initalPosition] = (byte) (mask(order, buff.get(i)) - 2);
    }
    return new String(decoded, UTF8);
  }
}
