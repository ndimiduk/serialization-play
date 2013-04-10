package types;


import java.math.BigInteger;
import java.nio.ByteBuffer;

import util.HSerializer;

public class VARINT extends HSerializer<BigInteger> {

  public VARINT() { super(); }
  public VARINT(Order order) { super(order); }

  @Override
  public byte[] toBytes(BigInteger val) {
    // TODO
    throw new RuntimeException("Not yet implemented.");
  }

  @Override
  public void putBytes(ByteBuffer buff, BigInteger val) {
    // TODO
    throw new RuntimeException("Not yet implemented.");
  }

  @Override
  public BigInteger fromBytes(byte[] bytes) {
    // TODO
    throw new RuntimeException("Not yet implemented.");
  }

}
