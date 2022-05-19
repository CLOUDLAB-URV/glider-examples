package org.example.benchmark;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.crail.CrailBuffer;
import org.apache.crail.CrailFile;
import org.apache.crail.CrailInputStream;
import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailObject;
import org.apache.crail.CrailObjectProxy;
import org.apache.crail.CrailOutputStream;
import org.apache.crail.CrailResult;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.CrailStore;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.core.ActiveAsyncChannel;
import org.apache.crail.core.ActiveReadableChannel;
import org.apache.crail.core.ActiveWritableChannel;
import org.apache.crail.memory.OffHeapBuffer;

public class ClientParallel {
  private CrailConfiguration conf;
  private CrailStore store;
  private BenchStats stats;
  private int experiments;
  private ExecutorService es;

  public ClientParallel(int experiemnts, int nParallel) {
    this.experiments = experiemnts;
    es = Executors.newFixedThreadPool(nParallel);
    try {
      conf = CrailConfiguration.createConfigurationFromFile();
      store = CrailStore.newInstance(conf);
      stats = new BenchStats();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    // Defaults:
    String type = "";
    String filename = "/tmpdata";
    int buffSize = 1024 * 1024;
    int iterations = 10240;
    int batch = 2;
    int experiments = 5;
    int nParallel = 1;

    String benchmarkTypes = "file|filea|action|actiona";
    Option typeOption = Option.builder("t").desc("type of experiment [" + benchmarkTypes + "]").hasArg().build();

    Option helpOption = Option.builder("h").desc("show this help message").build();
    Option fileOption = Option.builder("f").desc("path for crail file").hasArg().build();
    Option sizeOption = Option.builder("s").desc("buffer size [bytes]").hasArg().build();
    Option iterOption = Option.builder("k").desc("iterations [1..n]").hasArg().build();
    Option batchOption = Option.builder("b").desc("batch size [1..n]").hasArg().build();
    Option expOption = Option.builder("e").desc("number of experiments [1..n]").hasArg().build();
    Option parOption = Option.builder("p").desc("number of parallel actions [1..n]").hasArg().build();

    Options options = new Options();
    options.addOption(helpOption);
    options.addOption(typeOption);
    options.addOption(fileOption);
    options.addOption(sizeOption);
    options.addOption(iterOption);
    options.addOption(batchOption);
    options.addOption(expOption);
    options.addOption(parOption);

    CommandLineParser parser = new DefaultParser();
    try {
      CommandLine line = parser.parse(options, args);

      if (line.hasOption(helpOption.getOpt())) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("bench actions", options);
        System.exit(-1);
      }
      if (line.hasOption(typeOption.getOpt())) {
        type = line.getOptionValue(typeOption.getOpt());
      }
      if (line.hasOption(fileOption.getOpt())) {
        filename = line.getOptionValue(fileOption.getOpt());
      }
      if (line.hasOption(sizeOption.getOpt())) {
        buffSize = Integer.parseInt(line.getOptionValue(sizeOption.getOpt()));
      }
      if (line.hasOption(iterOption.getOpt())) {
        iterations = Integer.parseInt(line.getOptionValue(iterOption.getOpt()));
      }
      if (line.hasOption(batchOption.getOpt())) {
        batch = Integer.parseInt(line.getOptionValue(batchOption.getOpt()));
      }
      if (line.hasOption(expOption.getOpt())) {
        experiments = Integer.parseInt(line.getOptionValue(expOption.getOpt()));
      }
      if (line.hasOption(expOption.getOpt())) {
        nParallel = Integer.parseInt(line.getOptionValue(parOption.getOpt()));
      }

    } catch (ParseException e) {
      System.err.println("Could not parse options.");
      e.printStackTrace();
      System.exit(-1);
    }

    //////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////

    try {
      ClientParallel client = new ClientParallel(experiments, nParallel);

      if (type.equalsIgnoreCase("file")) {
        for (int i = 0; i < experiments; i++) {
          client.file(filename, buffSize, iterations, nParallel);
        }
      } else if (type.equalsIgnoreCase("filea")) {
        for (int i = 0; i < experiments; i++) {
          client.fileAsync(filename, buffSize, iterations, batch, nParallel);
        }
      } else if (type.equalsIgnoreCase("action")) {
        for (int i = 0; i < experiments; i++) {
          client.action(filename, buffSize, iterations, nParallel);
        }
      } else if (type.equalsIgnoreCase("actiona")) {
        for (int i = 0; i < experiments; i++) {
          client.actionAsync(filename, buffSize, iterations, batch, nParallel);
        }
      } else {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("bench actions", options);
        System.exit(-1);
      }
      System.out.printf("%nbenmark %s, buffer size %d, iterations %d, batch size %d, parallel %d, experiments %d%n",
          type, buffSize, iterations, batch, nParallel, experiments);
      client.showResults();

      client.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void showResults() {
    System.out.printf("Average results of %d experiments%n", experiments);
    System.out.println("Bandwidth");
    System.out.println("Read:  " + stats.getBwRead() + " Mbps");
    System.out.println("Write: " + stats.getBwWrite() + " Mbps");
  }

  private void file(String filename, int buffSize, int iterations, int par) throws Exception {
    List<CrailStore> stores = new ArrayList<>(par);
    Queue<CrailFile> files = new ConcurrentLinkedQueue<>();
    Queue<CrailBuffer> buffers = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < par; i++) {
      CrailStore store = CrailStore.newInstance(conf);
      stores.add(store);

      CrailFile crailFile = store.create(filename + i, CrailNodeType.DATAFILE, CrailStorageClass.get(1),
          CrailLocationClass.DEFAULT, false)
          .get().asFile();
      files.add(crailFile);
      buffers.add(OffHeapBuffer.wrap(ByteBuffer.allocateDirect(buffSize)));
    }
    List<Future<?>> taskFutures = new ArrayList<>(par);

    // Write
    Queue<CrailOutputStream> outStreams = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < par; i++) {
      CrailFile crailFile = files.poll();
      outStreams.add(crailFile.getDirectOutputStream(buffSize * iterations));
      files.add(crailFile);
    }

    long time1 = System.currentTimeMillis();
    for (int p = 0; p < par; p++) {
      taskFutures.add(es.submit(() -> {
        CrailBuffer buffer = buffers.poll();
        CrailOutputStream crailOutputStream = outStreams.poll();
        try {
          for (int i = 0; i < iterations; i++) {
            crailOutputStream.write(buffer).get();
            buffer.clear();
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
        outStreams.add(crailOutputStream);
        buffers.add(buffer);
      }));
    }
    for (Future<?> future : taskFutures) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }
    long time2 = System.currentTimeMillis();

    for (int i = 0; i < par; i++) {
      outStreams.poll().close();
    }

    double elapsedSecs = (double) (time2 - time1) / 1000;
    double kb = (double) buffSize * iterations * par / 1024;
    double bits = (double) buffSize * iterations * par * 8;

    System.out.println("Elapsed writing: " + elapsedSecs + " s");
    System.out.println("Bytes written: " + kb + " kb");
    System.out.println("Bandwidth: " + kb / elapsedSecs + " kb/s");
    System.out.println("Bandwidth: " + bits / elapsedSecs / 1000 / 1000 + " Mbps");
    stats.addBendwidthWrite(bits / elapsedSecs / 1000 / 1000);

    // Read
    taskFutures.clear();
    Queue<CrailInputStream> inStreams = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < par; i++) {
      CrailFile crailFile = files.poll();
      inStreams.add(crailFile.getDirectInputStream(buffSize * iterations));
      files.add(crailFile);
    }

    time1 = System.currentTimeMillis();
    for (int p = 0; p < par; p++) {
      taskFutures.add(es.submit(() -> {
        CrailBuffer buffer = buffers.poll();
        CrailInputStream crailInputStream = inStreams.poll();
        try {
          long read = 0;
          int r;
          for (int i = 0; i < iterations; i++) {
            buffer.clear();
            r = (int) crailInputStream.read(buffer).get().getLen();
            if (r > 0) {
              read += r;
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
        inStreams.add(crailInputStream);
        buffers.add(buffer);
      }));
    }
    for (Future<?> future : taskFutures) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }
    time2 = System.currentTimeMillis();

    for (int i = 0; i < par; i++) {
      inStreams.poll().close();
    }

    elapsedSecs = (double) (time2 - time1) / 1000;
    kb = (double) buffSize * iterations * par / 1024;
    bits = (double) buffSize * iterations * par * 8;

    System.out.println();
    System.out.println("Elapsed reading: " + elapsedSecs + " s");
    System.out.println("Bytes read: " + kb + " kb");
    System.out.println("Bandwidth: " + kb / elapsedSecs + " kb/s");
    System.out.println("Bandwidth: " + bits / elapsedSecs / 1000 / 1000 + " Mbps");
    stats.addBendwidthRead(bits / elapsedSecs / 1000 / 1000);

    // Clean up
    for (int i = 0; i < par; i++) {
      try {
        stores.get(i).delete(filename + i, true).get();
      } catch (Exception e) {
        System.out.println("Exception deleteing: " + e);
        // e.printStackTrace();
      }
      stores.get(i).close();
    }
  }

  private void fileAsync(String filename, int buffSize, int iterations, int batch, int par) throws Exception {
    List<CrailStore> stores = new ArrayList<>(par);
    Queue<CrailFile> files = new ConcurrentLinkedQueue<>();
    Queue<Queue<CrailBuffer>> buffers = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < par; i++) {
      CrailStore store = CrailStore.newInstance(conf);
      stores.add(store);

      CrailFile crailFile = store.create(filename + i, CrailNodeType.DATAFILE, CrailStorageClass.get(1),
          CrailLocationClass.DEFAULT, false)
          .get().asFile();
      files.add(crailFile);
      Queue<CrailBuffer> bufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
      for (int b = 0; b < batch; b++) {
        CrailBuffer buf = OffHeapBuffer.wrap(ByteBuffer.allocateDirect(buffSize));
        bufferQueue.add(buf);
      }
      buffers.add(bufferQueue);
    }
    List<Future<?>> taskFutures = new ArrayList<>(par);

    // Write
    long _loop = iterations;
    long _bufsize = CrailConstants.BUFFER_SIZE;
    long _capacity = _loop * _bufsize;
    Queue<CrailOutputStream> outStreams = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < par; i++) {
      CrailFile crailFile = files.poll();
      outStreams.add(crailFile.getDirectOutputStream(_capacity));
      files.add(crailFile);
    }

    long time1 = System.currentTimeMillis();
    for (int p = 0; p < par; p++) {
      taskFutures.add(es.submit(() -> {
        Queue<CrailBuffer> bufferQueue = buffers.poll();
        CrailOutputStream directStream = outStreams.poll();
        try {
          LinkedBlockingQueue<Future<CrailResult>> futureQueue = new LinkedBlockingQueue<>();
          HashMap<Integer, CrailBuffer> futureMap = new HashMap<>();
          double ops = 0;

          for (int i = 0; i < batch - 1 && ops < iterations; i++) {
            CrailBuffer buf = bufferQueue.poll();
            buf.clear();
            Future<CrailResult> future = directStream.write(buf);
            futureQueue.add(future);
            futureMap.put(future.hashCode(), buf);
            ops = ops + 1.0;
          }
          while (ops < iterations) {
            CrailBuffer buf = bufferQueue.poll();
            buf.clear();
            Future<CrailResult> future = directStream.write(buf);
            futureQueue.add(future);
            futureMap.put(future.hashCode(), buf);
            ops = ops + 1.0;

            future = futureQueue.poll();
            future.get();
            buf = futureMap.get(future.hashCode());
            bufferQueue.add(buf);
          }
          while (!futureQueue.isEmpty()) {
            Future<CrailResult> future = futureQueue.poll();
            future.get();
            CrailBuffer buf = futureMap.get(future.hashCode());
            bufferQueue.add(buf);
          }

        } catch (Exception e) {
          e.printStackTrace();
        }
        outStreams.add(directStream);
        buffers.add(bufferQueue);
      }));
    }
    for (Future<?> future : taskFutures) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }
    long time2 = System.currentTimeMillis();

    for (int i = 0; i < par; i++) {
      outStreams.poll().close();
    }

    double elapsedSecs = (double) (time2 - time1) / 1000;
    double kb = (double) buffSize * iterations * par / 1024;
    double bits = (double) buffSize * iterations * par * 8;

    System.out.println("Elapsed writing: " + elapsedSecs + " s");
    System.out.println("Bytes written: " + kb + " kb");
    System.out.println("Bandwidth: " + kb / elapsedSecs + " kb/s");
    System.out.println("Bandwidth: " + bits / elapsedSecs / 1000 / 1000 + " Mbps");
    System.out.println("Latency: " + elapsedSecs / iterations + " s");
    stats.addBendwidthWrite(bits / elapsedSecs / 1000 / 1000);
    stats.addLatencyWrite(elapsedSecs / iterations);

    // Read
    taskFutures.clear();
    Queue<CrailInputStream> inStreams = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < par; i++) {
      CrailFile crailFile = files.poll();
      inStreams.add(crailFile.getDirectInputStream(buffSize * iterations));
      files.add(crailFile);
    }

    time1 = System.currentTimeMillis();
    for (int p = 0; p < par; p++) {
      taskFutures.add(es.submit(() -> {
        Queue<CrailBuffer> bufferQueue = buffers.poll();
        CrailInputStream directStream = inStreams.poll();
        try {
          LinkedBlockingQueue<Future<CrailResult>> futureQueue = new LinkedBlockingQueue<Future<CrailResult>>();
          HashMap<Integer, CrailBuffer> futureMap = new HashMap<Integer, CrailBuffer>();
          double ops = 0.0;

          for (int i = 0; i < batch - 1 && ops < iterations; i++) {
            CrailBuffer buf = bufferQueue.poll();
            buf.clear();
            Future<CrailResult> future = directStream.read(buf);
            futureQueue.add(future);
            futureMap.put(future.hashCode(), buf);
            ops = ops + 1.0;
          }
          while (ops < iterations) {
            CrailBuffer buf = bufferQueue.poll();
            buf.clear();
            Future<CrailResult> future = directStream.read(buf);
            futureQueue.add(future);
            futureMap.put(future.hashCode(), buf);
            ops = ops + 1.0;
      
            future = futureQueue.poll();
            CrailResult result = future.get();
            buf = futureMap.get(future.hashCode());
            bufferQueue.add(buf);
            result.getLen();
          }
          while (!futureQueue.isEmpty()) {
            Future<CrailResult> future = futureQueue.poll();
            CrailResult result = future.get();
            CrailBuffer buf = futureMap.get(future.hashCode());
            bufferQueue.add(buf);
            result.getLen();
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
        inStreams.add(directStream);
        buffers.add(bufferQueue);
      }));
    }
    for (Future<?> future : taskFutures) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }
    time2 = System.currentTimeMillis();

    for (int i = 0; i < par; i++) {
      inStreams.poll().close();
    }

    elapsedSecs = (double) (time2 - time1) / 1000;
    kb = (double) buffSize * iterations * par / 1024;
    bits = (double) buffSize * iterations * par * 8;

    System.out.println();
    System.out.println("Elapsed reading: " + elapsedSecs + " s");
    System.out.println("Bytes read: " + kb + " kb");
    System.out.println("Bandwidth: " + kb / elapsedSecs + " kb/s");
    System.out.println("Bandwidth: " + bits / elapsedSecs / 1000 / 1000 + " Mbps");
    stats.addBendwidthRead(bits / elapsedSecs / 1000 / 1000);

    // Clean up
    for (int i = 0; i < par; i++) {
      try {
        stores.get(i).delete(filename + i, true).get();
      } catch (Exception e) {
        System.out.println("Exception deleteing: " + e);
        // e.printStackTrace();
      }
      stores.get(i).close();
    }
  }

  private void action(String filename, int buffSize, int iterations, int par) throws Exception {
    // Create
    List<CrailStore> stores = new ArrayList<>(par);
    Queue<CrailObjectProxy> proxies = new ConcurrentLinkedQueue<>();
    Queue<ByteBuffer> buffers = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < par; i++) {
      CrailStore store = CrailStore.newInstance(conf);
      stores.add(store);

      CrailObject obj = store.create(filename + i, CrailNodeType.OBJECT, CrailStorageClass.DEFAULT,
          CrailLocationClass.DEFAULT, false)
          .get().asObject();
      CrailObjectProxy proxy = obj.getProxy();
      proxy.create(BwAction.class);
      proxies.add(proxy);
      buffers.add(ByteBuffer.allocateDirect(buffSize));
    }
    List<Future<?>> taskFutures = new ArrayList<>(par);

    // Write
    Queue<ActiveWritableChannel> writeChannels = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < par; i++) {
      CrailObjectProxy proxy = proxies.poll();
      ActiveWritableChannel writableChannel = proxy.getWritableChannel();
      writeChannels.add(writableChannel);
      proxies.add(proxy);
    }

    long time1 = System.currentTimeMillis();
    for (int p = 0; p < par; p++) {
      taskFutures.add(es.submit(() -> {
        ByteBuffer buffer = buffers.poll();
        ActiveWritableChannel writableChannel = writeChannels.poll();
        try {
          for (long i = 0; i < iterations; i++) {
            writableChannel.write(buffer);
            buffer.clear();
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
        writeChannels.add(writableChannel);
        buffers.add(buffer);
      }));
    }
    for (Future<?> future : taskFutures) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }
    long time2 = System.currentTimeMillis();

    for (int i = 0; i < par; i++) {
      writeChannels.poll().close();
    }

    double elapsedSecs = (double) (time2 - time1) / 1000;
    double kb = (double) buffSize * iterations * par / 1024;
    double bits = (double) buffSize * iterations * par * 8;

    System.out.println("WRITE " + par);
    System.out.println("Elapsed writing: " + elapsedSecs + " s");
    System.out.println("Bytes written: " + kb + " kb");
    System.out.println("Bandwidth: " + kb / elapsedSecs + " kb/s");
    System.out.println("Bandwidth: " + bits / elapsedSecs / 1000 / 1000 + " Mbps");

    stats.addBendwidthWrite(bits / elapsedSecs / 1000 / 1000);

    // Read
    taskFutures.clear();
    Queue<ActiveReadableChannel> readChannels = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < par; i++) {
      CrailObjectProxy proxy = proxies.poll();
      ActiveReadableChannel readableChannel = proxy.getReadableChannel();
      readChannels.add(readableChannel);
      proxies.add(proxy);
    }

    time1 = System.currentTimeMillis();
    for (int p = 0; p < par; p++) {
      taskFutures.add(es.submit(() -> {
        ByteBuffer buffer = buffers.poll();
        ActiveReadableChannel readableChannel = readChannels.poll();
        try {
          long totalRead = 0;
          int currentRead;
          for (int i = 0; i < iterations; i++) {
            buffer.clear();
            currentRead = readableChannel.read(buffer);
            if (currentRead > 0) {
              totalRead += currentRead;
            }
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
        readChannels.add(readableChannel);
        buffers.add(buffer);
      }));
    }
    for (Future<?> future : taskFutures) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }
    time2 = System.currentTimeMillis();

    for (int i = 0; i < par; i++) {
      readChannels.poll().close();
    }

    elapsedSecs = (double) (time2 - time1) / 1000;
    kb = (double) buffSize * iterations * par / 1024;
    bits = (double) buffSize * iterations * par * 8;

    System.out.println();
    System.out.println("READ " + par);
    System.out.println("Elapsed reading: " + elapsedSecs + " s");
    System.out.println("Bytes read: " + kb + " kb");
    System.out.println("Bandwidth: " + kb / elapsedSecs + " kb/s");
    System.out.println("Bandwidth: " + bits / elapsedSecs / 1000 / 1000 + " Mbps");

    stats.addBendwidthRead(bits / elapsedSecs / 1000 / 1000);

    // Clean up
    for (int i = 0; i < par; i++) {
      proxies.poll().delete();
      try {
        stores.get(i).delete(filename + i, true).get();
      } catch (Exception e) {
        System.out.println("Exception deleteing: " + e);
        // e.printStackTrace();
      }
      stores.get(i).close();
    }
  }

  private void actionAsync(String filename, int buffSize, int iterations, int batch, int par) throws Exception {
    // Create
    List<CrailStore> stores = new ArrayList<>(par);
    Queue<CrailObjectProxy> proxies = new ConcurrentLinkedQueue<>();
    Queue<Queue<ByteBuffer>> buffers = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < par; i++) {
      CrailStore store = CrailStore.newInstance(conf);
      stores.add(store);

      CrailObject obj = store.create(filename + i, CrailNodeType.OBJECT, CrailStorageClass.DEFAULT,
          CrailLocationClass.DEFAULT, false)
          .get().asObject();
      CrailObjectProxy proxy = obj.getProxy();
      proxy.create(BwAction.class);
      proxies.add(proxy);
      Queue<ByteBuffer> bufferQueue = new LinkedList<>();
      for (int b = 0; b < batch; b++) {
        ByteBuffer buffer = ByteBuffer.allocateDirect(buffSize);
        bufferQueue.add(buffer);
      }
      buffers.add(bufferQueue);
    }
    List<Future<?>> taskFutures = new ArrayList<>(par);

    // Write
    Queue<ActiveAsyncChannel> writeChannels = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < par; i++) {
      CrailObjectProxy proxy = proxies.poll();
      ActiveAsyncChannel writableChannel = proxy.getWritableAsyncChannel();
      writeChannels.add(writableChannel);
      proxies.add(proxy);
    }

    long time1 = System.currentTimeMillis();
    for (int p = 0; p < par; p++) {
      taskFutures.add(es.submit(() -> {
        ActiveAsyncChannel writableChannel = writeChannels.poll();
        Queue<ByteBuffer> bufferQueue = buffers.poll();
        try {
          LinkedBlockingQueue<Future<Integer>> futureQueue = new LinkedBlockingQueue<>();
          HashMap<Integer, ByteBuffer> futureMap = new HashMap<>();
          double ops = 0.0;

          for (int i = 0; i < batch - 1 && ops < iterations; i++) {
            ByteBuffer buf = bufferQueue.poll();
            buf.clear();
            Future<Integer> future = writableChannel.write(buf);
            futureQueue.add(future);
            futureMap.put(future.hashCode(), buf);
            ops = ops + 1.0;
          }
          while (ops < iterations) {
            ByteBuffer buf = bufferQueue.poll();
            buf.clear();
            Future<Integer> future = writableChannel.write(buf);
            futureQueue.add(future);
            futureMap.put(future.hashCode(), buf);
            ops = ops + 1.0;

            future = futureQueue.poll();
            future.get();
            buf = futureMap.get(future.hashCode());
            bufferQueue.add(buf);
          }
          while (!futureQueue.isEmpty()) {
            Future<Integer> future = futureQueue.poll();
            future.get();
            ByteBuffer buf = futureMap.get(future.hashCode());
            bufferQueue.add(buf);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
        writeChannels.add(writableChannel);
        buffers.add(bufferQueue);
      }));
    }
    for (Future<?> future : taskFutures) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }
    long time2 = System.currentTimeMillis();

    for (int i = 0; i < par; i++) {
      writeChannels.poll().close();
    }

    double elapsedSecs = (double) (time2 - time1) / 1000;
    double kb = (double) buffSize * iterations * par / 1024;
    double bits = (double) buffSize * iterations * par * 8;

    System.out.println("Async WRITE " + par);
    System.out.println("Elapsed writing: " + elapsedSecs + " s");
    System.out.println("Bytes written: " + kb + " kb");
    System.out.println("Bandwidth: " + kb / elapsedSecs + " kb/s");
    System.out.println("Bandwidth: " + bits / elapsedSecs / 1000 / 1000 + " Mbps");
    stats.addBendwidthWrite(bits / elapsedSecs / 1000 / 1000);

    // Read
    taskFutures.clear();
    Queue<ActiveAsyncChannel> readChannels = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < par; i++) {
      CrailObjectProxy proxy = proxies.poll();
      ActiveAsyncChannel readableChannel = proxy.getReadableAsyncChannel();
      readChannels.add(readableChannel);
      proxies.add(proxy);
    }

    time1 = System.currentTimeMillis();
    for (int p = 0; p < par; p++) {
      taskFutures.add(es.submit(() -> {
        ActiveAsyncChannel readableChannel = readChannels.poll();
        Queue<ByteBuffer> bufferQueue = buffers.poll();
        try {
          LinkedBlockingQueue<Future<Integer>> futureQueue = new LinkedBlockingQueue<>();
          HashMap<Integer, ByteBuffer> futureMap = new HashMap<>();
          double ops = 0.0;
          for (int i = 0; i < batch - 1 && ops < iterations; i++) {
            ByteBuffer buf = bufferQueue.poll();
            buf.clear();
            Future<Integer> future = readableChannel.read(buf);
            futureQueue.add(future);
            futureMap.put(future.hashCode(), buf);
            ops = ops + 1.0;
          }
          while (ops < iterations) {
            ByteBuffer buf = bufferQueue.poll();
            buf.clear();
            Future<Integer> future = readableChannel.read(buf);
            futureQueue.add(future);
            futureMap.put(future.hashCode(), buf);
            ops = ops + 1.0;

            future = futureQueue.poll();
            int result = future.get();
            buf = futureMap.get(future.hashCode());
            bufferQueue.add(buf);
          }
          while (!futureQueue.isEmpty()) {
            Future<Integer> future = futureQueue.poll();
            int result = future.get();
            ByteBuffer buf = futureMap.get(future.hashCode());
            bufferQueue.add(buf);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
        readChannels.add(readableChannel);
        buffers.add(bufferQueue);
      }));
    }
    for (Future<?> future : taskFutures) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }
    time2 = System.currentTimeMillis();

    for (int i = 0; i < par; i++) {
      readChannels.poll().close();
    }

    elapsedSecs = (double) (time2 - time1) / 1000;
    kb = (double) buffSize * iterations * par / 1024;
    bits = (double) buffSize * iterations * par * 8;

    System.out.println();
    System.out.println("Async READ " + par);
    System.out.println("Elapsed reading: " + elapsedSecs + " s");
    System.out.println("Bytes read: " + kb + " kb");
    System.out.println("Bandwidth: " + kb / elapsedSecs + " kb/s");
    System.out.println("Bandwidth: " + bits / elapsedSecs / 1000 / 1000 + " Mbps");
    stats.addBendwidthRead(bits / elapsedSecs / 1000 / 1000);

    // Clean up
    for (int i = 0; i < par; i++) {
      proxies.poll().delete();
      try {
        stores.get(i).delete(filename + i, true).get();
      } catch (Exception e) {
        System.out.println("Exception deleteing: " + e);
        // e.printStackTrace();
      }
      stores.get(i).close();
    }
  }

  private void close() throws Exception {
    store.close();
    es.shutdown();
    try {
      if (!es.awaitTermination(30, TimeUnit.SECONDS)) {
        System.out.println("Executor did not terminate");
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
