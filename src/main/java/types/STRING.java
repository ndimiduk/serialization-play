package types;


import java.nio.ByteBuffer;

import util.HSerializer;

public class STRING extends HSerializer<String> {

  @Override
  public byte[] toBytes(String val) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void putBytes(ByteBuffer buff, String val) {
    // TODO Auto-generated method stub

  }

  @Override
  public String fromBytes(byte[] bytes) {
    // TODO Auto-generated method stub
    return null;
  }

  public static String toString(final byte[] bytes, final int offset, Order order) {
    return null;
  }

}
