package org.example;

import java.util.Arrays;

import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailObject;
import org.apache.crail.CrailObjectProxy;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.CrailStore;
import org.apache.crail.active.ActionWithFile;
import org.apache.crail.conf.CrailConfiguration;

/**
 * Test Write and Read to an Active Object
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
    byte[] dataBuf = {0, 1, 2, 4, 5};
//    byte[] dataBuf = new byte[2000000];
    proxy.write(dataBuf);

    // Read
    CrailObject crailObject = store.lookup(filename).get().asObject();
    CrailObjectProxy objectProxy = crailObject.getProxy();
    dataBuf = new byte[5];
    int read = objectProxy.read(dataBuf);

    System.out.println("Read " + read + " bytes");
    System.out.println("Contents: " + Arrays.toString(Arrays.copyOf(dataBuf, 5)));

    objectProxy.delete();
    store.close();
  }
}
