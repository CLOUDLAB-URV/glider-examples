package org.example.preprocessing.baseline;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
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

import org.apache.crail.CrailBufferedInputStream;
import org.apache.crail.CrailBufferedOutputStream;
import org.apache.crail.CrailFile;
import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNode;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.CrailStore;
import org.apache.crail.conf.CrailConfiguration;

public class Client {
  private static int N_WORKERS = 2;
  private static String FILE_FORM = "/Datasets/wiki512/AA/wiki_%02d";
  private static String CRAIL_FORM = "/wiki_%02d";

  private static void sendFileToCrail(String filename, CrailFile crailFile) {
    Path path = Paths.get(filename);
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
      CrailBufferedOutputStream crailBufferedOutputStream = crailFile.getBufferedOutputStream(channel.size());

      ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024); // 1 MiB buffer

      while (channel.read(buffer) != -1) {
        buffer.flip();
        crailBufferedOutputStream.write(buffer);
        buffer.rewind();
      }
      crailBufferedOutputStream.close();
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
        // Get full file from Crail
        String crailName = String.format(CRAIL_FORM, workerId);
        CrailFile crailFile = store.lookup(crailName).get().asFile();
        CrailBufferedInputStream cbis = crailFile.getBufferedInputStream(crailFile.getCapacity());

        // Filter data
        Stream<String> lines = new BufferedReader(new InputStreamReader(cbis)).lines();
        Stream<String> filteredLines = lines.filter(l -> l.contains("cosmos"));

        // Count words
        long nWords = filteredLines
            .flatMap(line -> Stream.of(line.toLowerCase().split("\\W+"))
                .filter(w -> !w.isEmpty()))
            .count();

        System.out.println("Worker " + workerId + " counted " + nWords +" words.");

        store.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
