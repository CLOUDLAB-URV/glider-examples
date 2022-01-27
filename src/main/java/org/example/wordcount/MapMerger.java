package org.example.wordcount;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
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
  public void onRead(ByteBuffer buffer) {
    // buffer will be populated with this action's serialized aggMap
    System.out.println("Reading merged map...");
    try {
      ByteArrayOutputStream byteOS = new ByteArrayOutputStream();
      ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteOS);
      objectOutputStream.writeObject(aggMap);
      buffer.put(byteOS.toByteArray());
    } catch (IOException e) {
      e.printStackTrace();
    } catch (BufferOverflowException e) {
      System.out.println("MapMerger action: map does not fit in read buffer.");
    }
  }

  @Override
  public int onWrite(ByteBuffer buffer) {
    // buffer contains a Map<String, Long> that should be merged into this action's aggMap
    System.out.println("Merging into map...");
    int size = buffer.remaining();
    try {
      ByteBuffer array = ByteBuffer.allocate(size).put(buffer);
      ByteArrayInputStream byteIS = new ByteArrayInputStream(array.array());
      ObjectInputStream inputStream = new ObjectInputStream(byteIS);
      @SuppressWarnings("unchecked")
      Map<String, Long> newMap = (Map<String, Long>) inputStream.readObject();
      newMap.forEach((key, value) -> aggMap.merge(key, value, Long::sum));
    } catch (IOException e) {
      e.printStackTrace();
      size = 0;
    } catch (ClassNotFoundException | ClassCastException e) {
      System.out.println("MapMerger action: Object sent is not a map.");
      e.printStackTrace();
      size = 0;
    }
    return size;
  }

  @Override
  public void onDelete() {
    super.onDelete();
  }
}
