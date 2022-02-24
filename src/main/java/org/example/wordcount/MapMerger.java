package org.example.wordcount;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.Map;

import org.apache.crail.CrailAction;

/**
 * Crail Action to merge <code>Map<String, Long></code> by summing values.
 */
public class MapMerger extends CrailAction {
  private Map<String, Long> aggMap;

  @Override
  public void onCreate() {
    aggMap = new HashMap<>();
  }

  @Override
  public void onReadStream(WritableByteChannel channel) {
    // this action's serialized aggMap is written to channel
    System.out.println("Reading merged map...");
    try {
      OutputStream stream = Channels.newOutputStream(channel);
      ObjectOutputStream oos = new ObjectOutputStream(stream);
      oos.writeObject(aggMap);
      oos.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void onWriteStream(ReadableByteChannel channel) {
    // channel contains a Map<String, Long> that should be merged into this action's aggMap
    System.out.println("Merging into map...");
    try {
      InputStream stream = Channels.newInputStream(channel);
      ObjectInputStream ois = new ObjectInputStream(stream);
      @SuppressWarnings("unchecked")
      Map<String, Long> newMap = (Map<String, Long>) ois.readObject();
      ois.close();
      newMap.forEach((key, value) -> aggMap.merge(key, value, Long::sum));
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException | ClassCastException e) {
      System.out.println("MapMerger action: Object sent is not a map.");
      e.printStackTrace();
    } finally {
      System.out.println("Finished merge");
    }
  }

  @Override
  public void onDelete() {
    super.onDelete();
  }
}
