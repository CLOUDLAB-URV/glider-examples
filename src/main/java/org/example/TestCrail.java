package org.example;

import java.util.Arrays;

import org.apache.crail.CrailBufferedInputStream;
import org.apache.crail.CrailBufferedOutputStream;
import org.apache.crail.CrailDirectory;
import org.apache.crail.CrailFile;
import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.CrailStore;
import org.apache.crail.conf.CrailConfiguration;

/**
 * Test a common Crail File (Write/Read) (not active)
 */
public class TestCrail {
  public static void main(String[] args) throws Exception {
    // Load configuration from crail home config
    CrailConfiguration conf = CrailConfiguration.createConfigurationFromFile();
    CrailStore store = CrailStore.newInstance(conf);
    store.getStatistics().print("start");

    String filename = "/test-file";

    try {
      store.delete(filename, true);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // Write
    CrailFile fileW = store.create(filename, CrailNodeType.DATAFILE, CrailStorageClass.DEFAULT,
                                   CrailLocationClass.DEFAULT, false)
                           .get().asFile();
    CrailBufferedOutputStream outputStream = fileW.getBufferedOutputStream(2000000);
    System.out.println(fileW.getPath() + " " + fileW.getFd());

//    byte[] dataBuf = {0, 1, 2, 4, 5};
    byte[] dataBuf = new byte[2000000];
    outputStream.write(dataBuf);
    outputStream.close();

    CrailDirectory dir = store.lookup("/").get().asDirectory();
    System.out.println(dir.files());
    dir.listEntries().forEachRemaining(System.out::println);

    // Read
    CrailFile fileR = store.lookup(filename).get().asFile();
    CrailBufferedInputStream bufferedStream = fileR.getBufferedInputStream(fileR.getCapacity());
    dataBuf = new byte[2000000];
    int read = bufferedStream.read(dataBuf);
    bufferedStream.close();

    System.out.println("Read " + read + " bytes");
    System.out.println("Contents: " + Arrays.toString(Arrays.copyOf(dataBuf, 5)));

    store.getStatistics().print("end");
    store.close();
  }
}
