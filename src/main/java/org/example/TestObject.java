package org.example;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailObject;
import org.apache.crail.CrailObjectProxy;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.CrailStore;
import org.apache.crail.active.ActionWithFile;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.core.ActiveReadableChannel;
import org.apache.crail.core.ActiveWritableChannel;

/**
 * Test Write and Read to an Active Object (direct write/read deprecated)
 */
public class TestObject {
  public static void main(String[] args) throws Exception {
    // Load configuration from crail home config
    CrailConfiguration conf = CrailConfiguration.createConfigurationFromFile();
    CrailStore store = CrailStore.newInstance(conf);

    String filename = "/test-object";

    try {
      store.delete(filename, true).get();
    } catch (Exception e) {
      e.printStackTrace();
    }

    // Create
    CrailObject obj = store.create(filename, CrailNodeType.OBJECT, CrailStorageClass.DEFAULT,
        CrailLocationClass.DEFAULT, false).get().asObject();
    obj.syncDir();
    CrailObjectProxy proxy = obj.getProxy();
    System.out.println("Path: " + obj.getPath() + ", id: " + obj.getFd());
    proxy.create(ActionWithFile.class);

    // Write
//    byte[] dataBuf = {0, 1, 2, 4, 5};
    byte[] dataBuf = new byte[2000000];
    ByteBuffer buffer = ByteBuffer.wrap(dataBuf);
    ActiveWritableChannel writableChannel = proxy.getWritableChannel();
    writableChannel.write(buffer);
    writableChannel.close();
//    proxy.write(dataBuf);

    // Read
    CrailObject crailObject = store.lookup(filename).get().asObject();
    CrailObjectProxy objectProxy = crailObject.getProxy();
//    dataBuf = new byte[5];
//    int read = objectProxy.read(dataBuf);
    ByteBuffer rBuffer = ByteBuffer.allocate(500000);
    ActiveReadableChannel readableChannel = objectProxy.getReadableChannel();
    int read = 0;
    int total = 0;
    while (read != -1) {
      total += read;
      rBuffer.clear();
      read = readableChannel.read(rBuffer);
      System.out.println("read = " + read);
      System.out.println("rBuffer = " + Arrays.toString(Arrays.copyOf(rBuffer.array(), 5)));
    }
    readableChannel.close();

    System.out.println("Read " + total + " bytes");

    objectProxy.delete();
    store.close();
  }
}
