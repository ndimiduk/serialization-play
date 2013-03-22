package types;


import java.math.BigInteger;
import java.nio.ByteBuffer;

import util.HSerializer;

public class DECIMAL extends HSerializer<BigInteger> {

  @Override
  public byte[] toBytes(BigInteger val) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void putBytes(ByteBuffer buff, BigInteger val) {
    // TODO Auto-generated method stub

  }

  @Override
  public BigInteger fromBytes(byte[] bytes) {
    // TODO Auto-generated method stub
    return null;
  }

}
