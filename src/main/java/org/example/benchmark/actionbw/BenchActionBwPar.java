package org.example.benchmark.actionbw;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailObject;
import org.apache.crail.CrailObjectProxy;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.CrailStore;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.core.ActiveReadableChannel;
import org.apache.crail.core.ActiveWritableChannel;

public class BenchActionBwPar {
  private static int N_ACTIONS = 1;
  private static long mbToWrite = 20 * 1024;

  public static void main(String[] args) throws Exception {
    // Load configuration from crail home config
    // CrailConfiguration conf = CrailConfiguration.createConfigurationFromFile();
    // CrailStore store = CrailStore.newInstance(conf);
    ExecutorService es = Executors.newFixedThreadPool(N_ACTIONS);

    // WRITE
    List<Future<?>> futures = new ArrayList<>(N_ACTIONS);
    long time1 = System.currentTimeMillis();

    for (int i = 0; i < N_ACTIONS; i++) {
      futures.add(es.submit(new WorkerBenchWrite(i)));
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
    double kb = (double) mbToWrite * 1024 * N_ACTIONS;

    System.out.println("WRITE " + N_ACTIONS);
    System.out.println("Elapsed writing: " + elapsedSecs + " s");
    System.out.println("Bytes written: " + kb + " KiB");
    System.out.println("Bandwidth: " + kb / elapsedSecs + " KiB/s");

    // READ
    futures.clear();
    time1 = System.currentTimeMillis();

    for (int i = 0; i < N_ACTIONS; i++) {
      futures.add(es.submit(new WorkerBenchRead(i)));
    }
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }

    time2 = System.currentTimeMillis();

    elapsedSecs = (double) (time2 - time1) / 1000;
    kb = (double) mbToWrite * 1024 * N_ACTIONS;

    System.out.println("READ");
    System.out.println("Elapsed reading: " + elapsedSecs + " s");
    System.out.println("Bytes read: " + kb + " KiB");
    System.out.println("Bandwidth: " + kb / elapsedSecs + " KiB/s");

    // store.close();

    es.shutdown();
    try {
      if (!es.awaitTermination(30, TimeUnit.SECONDS)) {
        System.out.println("Executor did not terminate");
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public static class WorkerBenchWrite implements Runnable {
    int workerN;

    public WorkerBenchWrite(int worker) {
      this.workerN = worker;
    }

    @Override
    public void run() {
      try {
        CrailConfiguration conf = CrailConfiguration.createConfigurationFromFile();
        CrailStore store = CrailStore.newInstance(conf);
        String filename = "/bench-action" + workerN;

        try {
          store.delete(filename, true).get();
        } catch (Exception e) {
          e.printStackTrace();
        }

        // Create
        CrailObject obj = store.create(filename, CrailNodeType.OBJECT, CrailStorageClass.DEFAULT,
            CrailLocationClass.DEFAULT, false)
            .get().asObject();
        CrailObjectProxy proxy = obj.getProxy();
        proxy.create(BwAction.class);

        // Write
        ActiveWritableChannel writableChannel = proxy.getWritableChannel();
        byte[] b = new byte[1024 * 1024];
        Random random = new Random();
        random.nextBytes(b);
        ByteBuffer buffer = ByteBuffer.allocateDirect(1024 * 1024); // 1 MB buffer
        buffer.put(b).clear();

        for (long writen = 0; writen < mbToWrite; writen++) {
          writableChannel.write(buffer);
          buffer.clear();
        }
        writableChannel.close();
        store.close();

      } catch (Exception e) {
        e.printStackTrace();
      }
    }

  }

  public static class WorkerBenchRead implements Runnable {
    int workerN;

    public WorkerBenchRead(int worker) {
      this.workerN = worker;
    }

    @Override
    public void run() {
      try {
        CrailConfiguration conf = CrailConfiguration.createConfigurationFromFile();
        CrailStore store = CrailStore.newInstance(conf);
        String filename = "/bench-action" + workerN;

        // Lookup
        CrailObject obj = store.lookup(filename)
            .get().asObject();
        CrailObjectProxy proxy = obj.getProxy();

        // Read
        ByteBuffer buffer = ByteBuffer.allocateDirect(1024 * 1024); // 1 MB buffer
        ActiveReadableChannel readableChannel = proxy.getReadableChannel();

        long totalRead = 0;
        int currentRead = readableChannel.read(buffer);
        while (currentRead != -1) {
          totalRead += currentRead;
          buffer.clear();
          currentRead = readableChannel.read(buffer);
        }
        readableChannel.close();

        double kb = (double) totalRead / 1024;
        System.out.println("Bytes read: " + kb + " KiB");

        proxy.delete();
        store.close();

      } catch (Exception e) {
        e.printStackTrace();
      }
    }

  }
}
