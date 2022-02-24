package org.example;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailObject;
import org.apache.crail.CrailObjectProxy;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.CrailStore;
import org.apache.crail.active.ActionWithFile;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.core.ActiveWritableChannel;

public class TestStream {
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
                                   CrailLocationClass.DEFAULT, false)
                           .get().asObject();
    obj.syncDir();
    CrailObjectProxy proxy = obj.getProxy();
    System.out.println("Path: " + obj.getPath() + ", id: " + obj.getFd());
    proxy.create(ActionWithFile.class);

    // Stream write
    Path path = Paths.get("/Datasets/wiki1/AA/wiki_00");
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
      ActiveWritableChannel writableChannel = proxy.getWritableChannel();
      ByteBuffer buffer = ByteBuffer.allocateDirect(16 * 1024); // 16 KB buffer

      while (channel.read(buffer) != -1) {
        System.out.println("buffer = " + buffer);
        buffer.flip();
        writableChannel.write(buffer);
        buffer.clear();
      }
      writableChannel.close();
    }
    System.out.println("Files.lines(path).count() = " + Files.lines(path).count());
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
      InputStream stream = Channels.newInputStream(channel);
      BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
//      System.out.println("file lines = " + reader.lines().count());
      reader.lines().forEach(l -> System.out.println(l.substring(1, 50)));
    }

    // Stream read
    InputStream is = proxy.getInputStream();
    BufferedReader reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(is, 16 * 1024)));
//		System.out.println("file lines = " + reader.lines().count());
    reader.lines().forEach(l -> System.out.println(l.substring(1, 50)));

    // Delete
    proxy.delete();
    store.close();

  }
}
