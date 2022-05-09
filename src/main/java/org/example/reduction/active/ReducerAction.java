package org.example.reduction.active;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.crail.CrailAction;

public class ReducerAction extends CrailAction {
  private Map<Integer, Long> result;

  @Override
  public void onCreate() {
    result = new HashMap<>();
  }

  @Override
  public void onWrite(ReadableByteChannel channel) {
    Stream<String> lines = new BufferedReader(new InputStreamReader(Channels.newInputStream(channel))).lines();
    lines.forEach(line -> {
      result.merge(
          Integer.parseInt(line.split(",")[0]),
          Long.parseLong(line.split(",")[1]),
          (val, acc) -> val + acc);
    });

  }

  @Override
  public void onRead(WritableByteChannel channel) {
    try {
      ObjectOutputStream oos = new ObjectOutputStream(Channels.newOutputStream(channel));
      oos.writeObject(result);
      oos.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
