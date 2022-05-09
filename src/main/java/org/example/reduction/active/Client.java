package org.example.reduction.active;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OutputStreamWriter;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNode;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailObjectProxy;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.CrailStore;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.core.ActiveReadableChannel;
import org.apache.crail.core.ActiveWritableChannel;

public class Client {
  private static int N_KEYS = 1024;
  private static int N_WORKERS = 10;
  private static long WORKER_OPS = 50000000;
  private static String ACTION_PATH = "/reducer";

  public static void main(String[] args) throws Exception {
    CrailConfiguration conf = CrailConfiguration.createConfigurationFromFile();
    CrailStore store = CrailStore.newInstance(conf);

    // Create action
    CrailNode crailNode = store.create(ACTION_PATH, CrailNodeType.OBJECT, CrailStorageClass.DEFAULT,
        CrailLocationClass.DEFAULT, false).get();
    CrailObjectProxy reduceAction = crailNode.asObject().getProxy();
    reduceAction.create(ReducerAction.class, true);

    long start = System.currentTimeMillis();

    // Run workers
    ExecutorService es = Executors.newFixedThreadPool(N_WORKERS);
    List<Future<?>> futures = new ArrayList<>(N_WORKERS);

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
    long mid = System.currentTimeMillis();

    // Get result from action
    // 1 MiB buffer
    ActiveReadableChannel channel = reduceAction.getReadableChannel();
    BufferedInputStream bis = new BufferedInputStream(Channels.newInputStream(channel), 1024*1024);
    ObjectInputStream oos = new ObjectInputStream(bis);
    Map<Integer, Long> result = (Map<Integer, Long>) oos.readObject();
    oos.close();

    long end = System.currentTimeMillis();

    System.out.println(result.keySet());

    // Elapsed time includes from launching workers until getting the reduced result
    double elapsedSecs = (double) (end - start) / 1000;
    double elapsedWorkers = (double) (mid - start) / 1000;
    System.out.println("Elapsed: " + elapsedSecs + " s");
    System.out.println("Elapsed workers: " + elapsedWorkers + " s");
    System.out.println("Result size: " + channel.getTotalRead());

    // Clean up
    es.shutdown();
    reduceAction.delete();
    try {
      store.delete(ACTION_PATH, true).get();
    } catch (Exception e) {
      e.printStackTrace();
    }

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
        // Each worker has a different store to simulate they are independent and avoid
        // network lock
        CrailConfiguration conf = CrailConfiguration.createConfigurationFromFile();
        CrailStore store = CrailStore.newInstance(conf);

        CrailObjectProxy proxy = store.lookup(ACTION_PATH).get().asObject().getProxy();

        Random random = new Random();
        ActiveWritableChannel channel = proxy.getWritableChannel();
        // 1MiB buffer
        BufferedOutputStream bos = new BufferedOutputStream(Channels.newOutputStream(channel), 1024 * 1024);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(bos));
        for (int i = 0; i < WORKER_OPS; i++) {
          try {
            writer.write(String.join(",", String.valueOf(random.nextInt(N_KEYS)), String.valueOf(random.nextLong())));
            writer.newLine();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
        writer.close();

        System.out.println("Total file " + workerId + ": " + channel.getTotalWritten());

        store.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
