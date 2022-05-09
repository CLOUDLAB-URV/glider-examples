package org.example.benchmark;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.crail.CrailAction;

/**
 * Action to benchmark bandwidth to an action.
 */
public class BwAction extends CrailAction {
  private long mbWriten = 0;

  @Override
  public void onWrite(ReadableByteChannel channel) {
    ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024); // 1MB buffer

    try {
      long totalRead = 0;
      int currentRead = channel.read(buffer);
      while (currentRead != -1) {
        totalRead += currentRead;
        buffer.clear();
        currentRead = channel.read(buffer);
        mbWriten++;
      }
      channel.close();
      System.out.println("Total writen: " + totalRead);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void onRead(WritableByteChannel channel) {
    try {
      ByteBuffer buffer = ByteBuffer.allocateDirect(1024 * 1024); // 1 MB buffer

      for (long writen = 0; writen < mbWriten; writen++) {
        channel.write(buffer);
        buffer.clear();
      }
      channel.close();
    } catch (Exception e) {
      System.out.println("Crail buffer error.");
      e.printStackTrace();
    }
  }
}
