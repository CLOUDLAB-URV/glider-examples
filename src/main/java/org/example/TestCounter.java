package org.example;

import java.nio.ByteBuffer;

import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailObject;
import org.apache.crail.CrailObjectProxy;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.CrailStore;
import org.apache.crail.active.CounterAction;
import org.apache.crail.conf.CrailConfiguration;

/**
 * Test a Crail Active Object basic counter
 */
public class TestCounter {
  public static void main(String[] args) throws Exception {
    // Load configuration from crail home config
    CrailConfiguration conf = CrailConfiguration.createConfigurationFromFile();
    CrailStore store = CrailStore.newInstance(conf);

    String crailPath = "/counter";

    try {
      store.delete(crailPath, true);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // Create
    CrailObject obj = store.create(crailPath, CrailNodeType.OBJECT, CrailStorageClass.DEFAULT,
        CrailLocationClass.DEFAULT, false).get().asObject();
    CrailObjectProxy proxy = obj.getProxy();
    System.out.println("Path: " + obj.getPath() + ", id: " + obj.getFd());
    proxy.create(CounterAction.class);

    // Add counter
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(4);
    proxy.write(buffer.array());
    proxy.write(buffer.array());

    // Get counter
    buffer = ByteBuffer.allocate(Long.BYTES);
    proxy.read(buffer.array());
    long count = buffer.getLong();
    System.out.println(count);
  }
}
