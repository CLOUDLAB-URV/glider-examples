package org.example;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Random;

import org.apache.crail.CrailBufferedInputStream;
import org.apache.crail.CrailBufferedOutputStream;
import org.apache.crail.CrailDirectory;
import org.apache.crail.CrailFile;
import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.CrailStore;
import org.apache.crail.conf.CrailConfiguration;

public class BenchCrail {
  public static void main(String[] args) throws Exception {
    // Load configuration from crail home config
    CrailConfiguration conf = CrailConfiguration.createConfigurationFromFile();
    CrailStore store = CrailStore.newInstance(conf);

    String crailPath = "/test-file";
    // String localFile = "/Datasets/wiki100/AA/wiki_00";
    long bytesToWrite = 1024 * 1024 * 1024;

    try {
      store.delete(crailPath, true);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // Write
    CrailFile crailFile = store.create(crailPath, CrailNodeType.DATAFILE, CrailStorageClass.get(1),
        CrailLocationClass.DEFAULT, false)
        .get().asFile();
    System.out.println(crailFile.getPath() + " " + crailFile.getFd());

    long time1 = System.currentTimeMillis();
    // long written = writeFileToCrail(localFile, crailFile);
    writeBytesToCrail(bytesToWrite, crailFile);
    long time2 = System.currentTimeMillis();

    double elapsedSecs = (double) (time2 - time1) / 1000;
    double kb = (double) bytesToWrite / 1024;

    System.out.println("Elapsed writing: " + elapsedSecs + " s");
    System.out.println("Bytes written: " + kb + " kb");
    System.out.println("Bandwidth: " + kb / elapsedSecs + " kb/s");

    // Read

    time1 = System.currentTimeMillis();
    long read = readFileToCrail(crailFile);
    time2 = System.currentTimeMillis();

    elapsedSecs = (double) (time2 - time1) / 1000;
    kb = (double) read / 1024;

    System.out.println();
    System.out.println("Elapsed reading: " + elapsedSecs + " s");
    System.out.println("Bytes read: " + kb + " kb");
    System.out.println("Bandwidth: " + kb / elapsedSecs + " kb/s");

    store.close();
  }

  /**
   * Loads a local file at <code>filename</code> up to Crail servers as a simple
   * <code>CrailFile</code>. Lazy read-write with 8 KB buffer.
   *
   * @param filename  Path to local file.
   * @param crailFile Crail file descriptor object.
   */
  private static long writeFileToCrail(String filename, CrailFile crailFile) {
    Path path = Paths.get(filename);
    long totalRead = 0;
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
      CrailBufferedOutputStream crailBufferedOutputStream = crailFile.getBufferedOutputStream(channel.size());

      ByteBuffer buffer = ByteBuffer.allocate(8 * 1024); // 8 KB buffer

      int currentRead = channel.read(buffer);
      while (currentRead != -1) {
        totalRead += currentRead;
        buffer.flip();
        crailBufferedOutputStream.write(buffer);
        buffer.clear();
        currentRead = channel.read(buffer);
      }
      crailBufferedOutputStream.close();
      return totalRead;
    } catch (IOException e) {
      System.out.println("Cannot open/send file: " + path);
      e.printStackTrace();
      return 0;
    } catch (Exception e) {
      System.out.println("Crail buffer error.");
      e.printStackTrace();
      return 0;
    }
  }

  /**
   * Reads a file from Crail servers. With 8 KB buffer.
   *
   * @param crailFile Crail file descriptor object.
   */
  private static long readFileToCrail(CrailFile crailFile) {
    long totalRead = 0;
    try {
      CrailBufferedInputStream crailBufferedInputStream = crailFile.getBufferedInputStream(8 * 1024);

      ByteBuffer buffer = ByteBuffer.allocate(8 * 1024); // 8 KB buffer

      int currentRead = crailBufferedInputStream.read(buffer);
      while (currentRead != -1) {
        totalRead += currentRead;
        // buffer.flip();
        buffer.clear();
        currentRead = crailBufferedInputStream.read(buffer);
      }
      crailBufferedInputStream.close();
      return totalRead;
    } catch (Exception e) {
      System.out.println("Crail buffer error.");
      e.printStackTrace();
      return 0;
    }
  }

  /**
   * Writes bytes to a crail file. With 8 KB buffer.
   *
   * @param bytes     number of bytes to write.
   * @param crailFile Crail file descriptor object.
   */
  private static void writeBytesToCrail(long bytes, CrailFile crailFile) {
    try {
      CrailBufferedOutputStream crailBufferedOutputStream = crailFile.getBufferedOutputStream(bytes);

      byte[] b = new byte[8 * 1024];
      Random random = new Random();
      random.nextBytes(b);
      ByteBuffer buffer = ByteBuffer.wrap(b); // 8 KB buffer

      long written = 0;
      while (written < bytes) {
        if (written + buffer.capacity() > bytes) {
          buffer.limit((int) (bytes - written));
        }
        written += buffer.remaining();
        crailBufferedOutputStream.write(buffer);
        buffer.clear();
      }
      crailBufferedOutputStream.close();
    } catch (Exception e) {
      System.out.println("Crail buffer error.");
      e.printStackTrace();
    }
  }
}
