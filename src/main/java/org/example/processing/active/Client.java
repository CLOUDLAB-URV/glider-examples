package org.example.processing.active;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.crail.CrailBufferedOutputStream;
import org.apache.crail.CrailFile;
import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNode;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailObjectProxy;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.CrailStore;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.core.ActiveReadableChannel;

public class Client {
  private static int N_WORKERS = 10;
  private static String FILE_FORM = "/tmp/daniel-data/wiki1G/AA/wiki_%02d";
  private static String CRAIL_FORM = "/wiki_%02d";
  private static String ACTION_FORM = "/filter_%02d";

  private static void sendFileToCrail(String filename, CrailFile crailFile) {
    Path path = Paths.get(filename);
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
      CrailBufferedOutputStream crailBufferedOutputStream = crailFile.getBufferedOutputStream(channel.size());

      ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024); // 1 MiB buffer

      long total = 0;
      long time1 = System.currentTimeMillis();
      while (channel.read(buffer) != -1) {
        buffer.flip();
        total += buffer.remaining();
        crailBufferedOutputStream.write(buffer);
        buffer.rewind();
      }
      crailBufferedOutputStream.close();
      long time2 = System.currentTimeMillis();
      double elapsed = (double) (time2 - time1) / 1000.0;

      System.out.println("Written " + total + " bytes in " + elapsed + " s = "
          + (total * 8 / elapsed / 1000 / 1000) + " Mbps");

    } catch (IOException e) {
      System.out.println("Cannot open/send file: " + path);
      e.printStackTrace();
    } catch (Exception e) {
      System.out.println("Crail buffer error.");
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws Exception {
    // Load configuration from crail home config
    CrailConfiguration conf = CrailConfiguration.createConfigurationFromFile();
    CrailStore store = CrailStore.newInstance(conf);

    // Upload initial file(s)
    CrailFile crailFile;
    String filename;
    String crailName;
    for (int i = 0; i < N_WORKERS; i++) {
      filename = String.format(FILE_FORM, i);
      crailName = String.format(CRAIL_FORM, i);

      CrailNode crailNode = store.lookup(crailName).get();
      if (crailNode == null) {
        System.out.println("Loading new file to crail: " + crailName);
        crailFile = store.create(crailName, CrailNodeType.DATAFILE,
            CrailStorageClass.get(1), CrailLocationClass.DEFAULT, false)
            .get().asFile();
        sendFileToCrail(filename, crailFile);
        System.out.println("Done.");
      }
    }

    // Create Actions
    String actionName;
    CrailNode crailNode;
    CrailObjectProxy filterAction;
    for (int i = 0; i < N_WORKERS; i++) {
      actionName = String.format(ACTION_FORM, i);

      crailNode = store.lookup(actionName).get();
      if (crailNode == null) {
        crailNode = store.create(actionName, CrailNodeType.OBJECT, CrailStorageClass.DEFAULT,
            CrailLocationClass.DEFAULT, false).get();
        filterAction = crailNode.asObject().getProxy();
        filterAction.create(FilterAction.class);
      }
    }

    // Run workers
    ExecutorService es = Executors.newFixedThreadPool(N_WORKERS);
    List<Future<?>> futures = new ArrayList<>(N_WORKERS);
    long time1 = System.currentTimeMillis();

    for (int i = 0; i < N_WORKERS; i++) {
      futures.add(es.submit(new Worker(i)));
    }
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }

    long time2 = System.currentTimeMillis();

    double elapsedSecs = (double) (time2 - time1) / 1000;
    System.out.println("Elapsed: " + elapsedSecs + " s");

    // Clean up
    es.shutdown();
    try {
      if (!es.awaitTermination(30, TimeUnit.SECONDS)) {
        System.out.println("Executor did not terminate");
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    try {
      store.close();
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  public static class Worker implements Runnable {
    private int workerId;

    public Worker(int id) {
      this.workerId = id;
    }

    @Override
    public void run() {
      try {
        CrailConfiguration conf = CrailConfiguration.createConfigurationFromFile();
        CrailStore store = CrailStore.newInstance(conf);
        // Read filtered data from Crail Action
        String actionName = String.format(ACTION_FORM, workerId);
        CrailObjectProxy filterProxy = store.lookup(actionName).get().asObject().getProxy();
        ActiveReadableChannel readChannel = filterProxy.getReadableChannel();

        long time1 = System.currentTimeMillis();
        Stream<String> filteredLines = new BufferedReader(new InputStreamReader(Channels.newInputStream(readChannel)))
            .lines();

        // Count words
        long nWords = filteredLines
            .flatMap(line -> Stream.of(line.toLowerCase().split("\\W+"))
                .filter(w -> !w.isEmpty()))
            .count();
        long time2 = System.currentTimeMillis();
        double elapsed = (double) (time2 - time1) / 1000.0;

        System.out.println("Worker " + workerId + " counted " + nWords + " words.");
        System.out.println("Read " + readChannel.getTotalRead() + " bytes from action.");
        String crailName = String.format(CRAIL_FORM, workerId);
        CrailFile crailFile = store.lookup(crailName).get().asFile();
        System.out.println("Processed " + crailFile.getCapacity() + " bytes in " + elapsed + " s = "
            + (crailFile.getCapacity() * 8 / elapsed / 1000 / 1000) + " Mbps");

        store.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

}
