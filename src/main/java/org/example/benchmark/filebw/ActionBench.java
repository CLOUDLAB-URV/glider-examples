package org.example.benchmark.filebw;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Random;

import org.apache.crail.CrailAction;
import org.apache.crail.CrailBufferedInputStream;
import org.apache.crail.CrailBufferedOutputStream;
import org.apache.crail.CrailFile;
import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailStorageClass;

/**
 * Benchmarks accessing a file from an action.
 */
public class ActionBench extends CrailAction {
  public static final String FILE_SUFFIX = "-data";
  private CrailFile crailFile;
  private long mbToWrite = 50 * 1024;

  @Override
  public void onCreate() {
    try {
      crailFile = this.fs.create(this.self.getPath() + FILE_SUFFIX, CrailNodeType.DATAFILE,
          CrailStorageClass.get(1), CrailLocationClass.DEFAULT, false)
          .get().asFile();
    } catch (Exception e) {
      System.out.println("Error creating data file for action " + self.getPath());
      e.printStackTrace();
    }
  }

  @Override
  public void onRead(WritableByteChannel channel) {
    long time1 = System.currentTimeMillis();
    long read = readFileFromCrail(crailFile);
    long time2 = System.currentTimeMillis();

    double elapsedSecs = (double) (time2 - time1) / 1000;
    double kb = (double) read / 1024;

    System.out.println("READ");
    System.out.println("Elapsed reading: " + elapsedSecs + " s");
    System.out.println("Bytes read: " + kb + " kb");
    System.out.println("Bandwidth: " + kb / elapsedSecs + " kb/s");
    try {
      ByteBuffer buff = ByteBuffer.allocate(Integer.BYTES);
      buff.putInt(0).flip();
      channel.write(buff);
      channel.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void onWrite(ReadableByteChannel channel) {
    long time1 = System.currentTimeMillis();
    writeBytesToCrail(mbToWrite, crailFile);
    long time2 = System.currentTimeMillis();

    double elapsedSecs = (double) (time2 - time1) / 1000;
    double kb = (double) mbToWrite * 1024;

    System.out.println("WRITE");
    System.out.println("Elapsed writing: " + elapsedSecs + " s");
    System.out.println("Bytes written: " + kb + " kb");
    System.out.println("Bandwidth: " + kb / elapsedSecs + " kb/s");
    try {
      channel.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void onDelete() {
    try {
      this.fs.delete(crailFile.getPath(), true).get();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Reads a file from Crail servers.
   *
   * @param crailFile Crail file descriptor object.
   */
  static long readFileFromCrail(CrailFile crailFile) {
    long totalRead = 0;
    try {
      CrailBufferedInputStream crailBufferedInputStream = crailFile.getBufferedInputStream(8 * 1024);

      ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024); // 1MB buffer

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
   * Writes bytes to a crail file.
   *
   * @param mb        number of megabytes to write.
   * @param crailFile Crail file descriptor object.
   */
  static void writeBytesToCrail(long mb, CrailFile crailFile) {
    try {
      CrailBufferedOutputStream crailBufferedOutputStream = crailFile.getBufferedOutputStream(mb*1024*1024);

      byte[] b = new byte[1024 * 1024];
      Random random = new Random();
      random.nextBytes(b);
      ByteBuffer buffer = ByteBuffer.allocateDirect(1024*1024); // 1 MB buffer
      buffer.put(b).clear();      

      for (long writen = 0; writen < mb; writen++) {
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
