package types;


import java.nio.ByteBuffer;
import java.util.Date;

import util.HSerializer;

public class DATETIME extends HSerializer<Date> {

  @Override
  public byte[] toBytes(Date val) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void putBytes(ByteBuffer buff, Date val) {
    // TODO Auto-generated method stub

  }

  @Override
  public Date fromBytes(byte[] bytes) {
    // TODO Auto-generated method stub
    return null;
  }

}
