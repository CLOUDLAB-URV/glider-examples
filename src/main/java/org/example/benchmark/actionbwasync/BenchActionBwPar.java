package org.example.benchmark.actionbwasync;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailObject;
import org.apache.crail.CrailObjectProxy;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.CrailStore;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.core.ActiveAsyncChannel;
import org.example.benchmark.BwAction;

public class BenchActionBwPar {
  private static int N_ACTIONS = 32;
  private static int bufferSize = 1024 * 1024;
  private static long totalBytes = 10737418240L; // 10 GiB
  private static long operations = totalBytes / bufferSize;

  public static void main(String[] args) throws Exception {
    // Load configuration from crail home config
    CrailConfiguration conf = CrailConfiguration.createConfigurationFromFile();
    ExecutorService es = Executors.newFixedThreadPool(N_ACTIONS);

    List<CrailStore> stores = new ArrayList<>(N_ACTIONS);
    List<CrailObjectProxy> proxies = new ArrayList<>(N_ACTIONS);
    for (int i = 0; i < N_ACTIONS; i++) {
      CrailStore store = CrailStore.newInstance(conf);
      stores.add(store);

      String filename = "/bench-action" + i;

      // Create
      CrailObject obj = store.create(filename, CrailNodeType.OBJECT, CrailStorageClass.DEFAULT,
          CrailLocationClass.DEFAULT, false)
          .get().asObject();
      CrailObjectProxy proxy = obj.getProxy();
      proxy.create(BwAction.class);
      proxies.add(proxy);

    }

    // WRITE
    List<Future<?>> futures = new ArrayList<>(N_ACTIONS);
    long time1 = System.currentTimeMillis();

    for (int i = 0; i < N_ACTIONS; i++) {
      futures.add(es.submit(new WorkerBenchWrite(proxies.get(i))));
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
    double mb = (double) bufferSize * operations * N_ACTIONS / 1024 / 1024;
    double bits = (double) bufferSize * operations * N_ACTIONS * 8;

    System.out.println("WRITE " + N_ACTIONS);
    System.out.println("Elapsed writing: " + elapsedSecs + " s");
    System.out.println("Bytes written: " + mb + " mb");
    System.out.println("Bandwidth: " + mb / elapsedSecs + " mb/s");
    System.out.println("Bandwidth: " + bits / elapsedSecs / 1000 / 1000 + " Mbps");
    System.out.println("Latency: " + elapsedSecs / (operations * N_ACTIONS) + " s");

    // READ
    futures.clear();
    time1 = System.currentTimeMillis();

    for (int i = 0; i < N_ACTIONS; i++) {
      futures.add(es.submit(new WorkerBenchRead(proxies.get(i))));
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
    mb = (double) bufferSize * operations * N_ACTIONS / 1024 / 1024;
    bits = (double) bufferSize * operations * N_ACTIONS * 8;

    System.out.println("READ " + N_ACTIONS);
    System.out.println("Elapsed reading: " + elapsedSecs + " s");
    System.out.println("Bytes read: " + mb + " mb");
    System.out.println("Bandwidth: " + mb / elapsedSecs + " mb/s");
    System.out.println("Bandwidth: " + bits / elapsedSecs / 1000 / 1000 + " Mbps");
    System.out.println("Latency: " + elapsedSecs / (operations * N_ACTIONS) + " s");

    for (int i = 0; i < N_ACTIONS; i++) {
      String filename = "/bench-action" + i;
      proxies.get(i).delete();
      try {
        stores.get(i).delete(filename, true).get();
      } catch (Exception e) {
        System.out.println("Exception deleteing: " + e);
        // e.printStackTrace();
      }
      stores.get(i).close();
    }

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
    private CrailObjectProxy proxy;

    public WorkerBenchWrite(CrailObjectProxy proxy) {
      this.proxy = proxy;
    }

    @Override
    public void run() {
      try {
        // Write
        ActiveAsyncChannel writableChannel = proxy.getWritableAsyncChannel();

        ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize);

        List<Future<Integer>> futures = new LinkedList<>();
        for (long i = 0; i < operations; i++) {
          futures.add(writableChannel.write(buffer));
          buffer.clear();
        }
        futures.forEach(f -> {
          try {
            f.get();
          } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
          }
        });
        writableChannel.close();

      } catch (Exception e) {
        e.printStackTrace();
      }
    }

  }

  public static class WorkerBenchRead implements Runnable {
    private CrailObjectProxy proxy;

    public WorkerBenchRead(CrailObjectProxy proxy) {
      this.proxy = proxy;
    }

    @Override
    public void run() {
      try {
        // Read
        ActiveAsyncChannel readableChannel = proxy.getReadableAsyncChannel();
        ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize);

        List<Future<Integer>> futures = new LinkedList<>();
        for (int i = 0; i < operations; i++) {
          buffer.clear();
          futures.add(readableChannel.read(buffer));
        }
        long totalRead = futures.stream().collect(Collectors.summingLong(f -> {
          try {
            return f.get().longValue();
          } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return 0;
          }
        }));
        readableChannel.close();

        double kb = (double) totalRead / 1024;
        System.out.println("Bytes read: " + kb + " KiB");

      } catch (Exception e) {
        e.printStackTrace();
      }
    }

  }
}
