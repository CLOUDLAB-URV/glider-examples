package org.example.benchmark;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

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

public class Client {
  private CrailConfiguration conf;
  private CrailStore store;
  private BenchStats stats;
  private int experiments;

  public Client(int experiemnts) {
    this.experiments = experiemnts;
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
    int iterations = 10000;
    int batch = 4;
    int experiments = 5;

    String benchmarkTypes = "file|filea|action|actiona";
    Option typeOption = Option.builder("t").desc("type of experiment [" + benchmarkTypes + "]").hasArg().build();

    Option helpOption = Option.builder("h").desc("show this help message").build();
    Option fileOption = Option.builder("f").desc("path for crail file").hasArg().build();
    Option sizeOption = Option.builder("s").desc("buffer size [bytes]").hasArg().build();
    Option iterOption = Option.builder("k").desc("iterations [1..n]").hasArg().build();
    Option batchOption = Option.builder("b").desc("batch size [1..n]").hasArg().build();
    Option expOption = Option.builder("e").desc("number of experiments [1..n]").hasArg().build();

    Options options = new Options();
    options.addOption(helpOption);
    options.addOption(typeOption);
    options.addOption(fileOption);
    options.addOption(sizeOption);
    options.addOption(iterOption);
    options.addOption(batchOption);
    options.addOption(expOption);

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

    } catch (ParseException e) {
      System.err.println("Could not parse options.");
      e.printStackTrace();
      System.exit(-1);
    }

    //////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////

    try {
      Client client = new Client(experiments);

      if (type.equalsIgnoreCase("file")) {
        for (int i = 0; i < experiments; i++) {
          client.file(filename, buffSize, iterations);
        }
      } else if (type.equalsIgnoreCase("filea")) {
        for (int i = 0; i < experiments; i++) {
          client.fileAsync(filename, buffSize, iterations, batch);
        }
      } else if (type.equalsIgnoreCase("action")) {
        for (int i = 0; i < experiments; i++) {
          client.action(filename, buffSize, iterations);
        }
      } else if (type.equalsIgnoreCase("actiona")) {
        for (int i = 0; i < experiments; i++) {
          client.actionAsync(filename, buffSize, iterations, batch);
        }
      } else {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("bench actions", options);
        System.exit(-1);
      }
      System.out.printf("\nbenmark %s, buffer size %d, iterations %d, batch size %d, experiments %d\n",
          type, buffSize, iterations, batch, experiments);
      client.showResults();

      client.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void showResults() {
    System.out.printf("Average results of %d experiments\n", experiments);
    System.out.println("Bandwidth");
    System.out.println("Read:  " + stats.getBwRead() + " Mbps");
    System.out.println("Write: " + stats.getBwWrite() + " Mbps");
    System.out.println("Latency");
    System.out.println("Read:  " + stats.getLatRead() + " s");
    System.out.println("Write: " + stats.getLatWrtie() + " s");
  }

  private void file(String filename, int buffSize, int iterations) throws Exception {
    CrailFile crailFile = store.create(filename, CrailNodeType.DATAFILE, CrailStorageClass.get(1),
        CrailLocationClass.DEFAULT, false)
        .get().asFile();
    System.out.println(crailFile.getPath() + " " + crailFile.getFd());

    // Write
    CrailOutputStream crailOutputStream = crailFile.getDirectOutputStream(buffSize * iterations);
    CrailBuffer buffer = OffHeapBuffer.wrap(ByteBuffer.allocateDirect(buffSize));

    long time1 = System.currentTimeMillis();
    for (int i = 0; i < iterations; i++) {
      crailOutputStream.write(buffer).get();
      buffer.clear();
    }
    long time2 = System.currentTimeMillis();
    crailOutputStream.close();

    double elapsedSecs = (double) (time2 - time1) / 1000;
    double kb = (double) buffSize * iterations / 1024;
    double bits = (double) buffSize * iterations * 8;

    System.out.println("Elapsed writing: " + elapsedSecs + " s");
    System.out.println("Bytes written: " + kb + " kb");
    System.out.println("Bandwidth: " + kb / elapsedSecs + " kb/s");
    System.out.println("Bandwidth: " + bits / elapsedSecs / 1000 / 1000 + " Mbps");
    System.out.println("Latency: " + elapsedSecs / iterations + " s");
    stats.addBendwidthWrite(bits / elapsedSecs / 1000 / 1000);
    stats.addLatencyWrite(elapsedSecs / iterations);

    // Read
    CrailInputStream crailInputStream = crailFile.getDirectInputStream(buffSize * iterations);

    time1 = System.currentTimeMillis();
    long read = 0;
    int r;
    for (int i = 0; i < iterations; i++) {
      buffer.clear();
      r = (int) crailInputStream.read(buffer).get().getLen();
      if (r > 0) {
        read += r;
      }
    }
    time2 = System.currentTimeMillis();
    crailInputStream.close();

    elapsedSecs = (double) (time2 - time1) / 1000;
    kb = (double) read / 1024;
    bits = (double) read * 8;

    System.out.println();
    System.out.println("Elapsed reading: " + elapsedSecs + " s");
    System.out.println("Bytes read: " + kb + " kb");
    System.out.println("Bandwidth: " + kb / elapsedSecs + " kb/s");
    System.out.println("Bandwidth: " + bits / elapsedSecs / 1000 / 1000 + " Mbps");
    System.out.println("Latency: " + elapsedSecs / iterations + " s");
    stats.addBendwidthRead(bits / elapsedSecs / 1000 / 1000);
    stats.addLatencyRead(elapsedSecs / iterations);

    try {
      store.delete(filename, true).get();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void fileAsync(String filename, int buffSize, int iterations, int batch) throws Exception {
    CrailFile crailFile = store.create(filename, CrailNodeType.DATAFILE, CrailStorageClass.get(1),
        CrailLocationClass.DEFAULT, false)
        .get().asFile();
    System.out.println(crailFile.getPath() + " " + crailFile.getFd());

    ConcurrentLinkedQueue<CrailBuffer> bufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
    for (int i = 0; i < batch; i++) {
      CrailBuffer buf = OffHeapBuffer.wrap(ByteBuffer.allocateDirect(buffSize));
      bufferQueue.add(buf);
    }

    // Write
    LinkedBlockingQueue<Future<CrailResult>> futureQueue = new LinkedBlockingQueue<Future<CrailResult>>();
    HashMap<Integer, CrailBuffer> futureMap = new HashMap<Integer, CrailBuffer>();
    long _loop = (long) iterations;
    long _bufsize = (long) CrailConstants.BUFFER_SIZE;
    long _capacity = _loop * _bufsize;
    double ops = 0;
    CrailOutputStream directStream = crailFile.getDirectOutputStream(_capacity);

    long time1 = System.currentTimeMillis();
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
    }
    long time2 = System.currentTimeMillis();

    directStream.close();

    double elapsedSecs = (double) (time2 - time1) / 1000;
    double kb = (double) buffSize * iterations / 1024;
    double bits = (double) buffSize * iterations * 8;

    System.out.println("Elapsed writing: " + elapsedSecs + " s");
    System.out.println("Bytes written: " + kb + " kb");
    System.out.println("Bandwidth: " + kb / elapsedSecs + " kb/s");
    System.out.println("Bandwidth: " + bits / elapsedSecs / 1000 / 1000 + " Mbps");
    System.out.println("Latency: " + elapsedSecs / iterations + " s");
    stats.addBendwidthWrite(bits / elapsedSecs / 1000 / 1000);
    stats.addLatencyWrite(elapsedSecs / iterations);

    // Read
    bufferQueue.clear();
    for (int i = 0; i < batch; i++) {
      CrailBuffer buf = OffHeapBuffer.wrap(ByteBuffer.allocateDirect(buffSize));
      bufferQueue.add(buf);
    }

    double totalRead = 0.0;
    ops = 0;
    CrailInputStream directIStream = crailFile.getDirectInputStream(crailFile.getCapacity());
    futureMap.clear();
    futureQueue.clear();

    time1 = System.currentTimeMillis();
    for (int i = 0; i < batch - 1 && ops < iterations; i++) {
      CrailBuffer buf = bufferQueue.poll();
      buf.clear();
      Future<CrailResult> future = directIStream.read(buf);
      futureQueue.add(future);
      futureMap.put(future.hashCode(), buf);
      ops = ops + 1.0;
    }
    while (ops < iterations) {
      CrailBuffer buf = bufferQueue.poll();
      buf.clear();
      Future<CrailResult> future = directIStream.read(buf);
      futureQueue.add(future);
      futureMap.put(future.hashCode(), buf);
      ops = ops + 1.0;

      future = futureQueue.poll();
      CrailResult result = future.get();
      buf = futureMap.get(future.hashCode());
      bufferQueue.add(buf);
      totalRead += result.getLen();
    }
    while (!futureQueue.isEmpty()) {
      Future<CrailResult> future = futureQueue.poll();
      CrailResult result = future.get();
      futureMap.get(future.hashCode());
      totalRead += result.getLen();
    }
    time2 = System.currentTimeMillis();

    directIStream.close();

    elapsedSecs = (double) (time2 - time1) / 1000;
    kb = (double) totalRead / 1024;
    bits = (double) totalRead * 8;

    System.out.println();
    System.out.println("Elapsed reading: " + elapsedSecs + " s");
    System.out.println("Bytes read: " + kb + " kb");
    System.out.println("Bandwidth: " + kb / elapsedSecs + " kb/s");
    System.out.println("Bandwidth: " + bits / elapsedSecs / 1000 / 1000 + " Mbps");
    System.out.println("Latency: " + elapsedSecs / iterations + " s");
    stats.addBendwidthRead(bits / elapsedSecs / 1000 / 1000);
    stats.addLatencyRead(elapsedSecs / iterations);

    try {
      store.delete(filename, true).get();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void action(String filename, int buffSize, int iterations) throws Exception {
    // Create
    CrailObject obj = store.create(filename, CrailNodeType.OBJECT, CrailStorageClass.DEFAULT,
        CrailLocationClass.DEFAULT, false)
        .get().asObject();
    CrailObjectProxy proxy = obj.getProxy();
    proxy.create(BwAction.class);

    // Write
    ActiveWritableChannel writableChannel = proxy.getWritableChannel();

    ByteBuffer buffer = ByteBuffer.allocateDirect(buffSize);

    long time1 = System.currentTimeMillis();

    for (long i = 0; i < iterations; i++) {
      writableChannel.write(buffer);
      buffer.clear();
    }
    writableChannel.close();

    long time2 = System.currentTimeMillis();

    double elapsedSecs = (double) (time2 - time1) / 1000;
    double kb = (double) buffSize * iterations / 1024;
    double bits = (double) buffSize * iterations * 8;

    System.out.println("WRITE");
    System.out.println("Elapsed writing: " + elapsedSecs + " s");
    System.out.println("Bytes written: " + kb + " kb");
    System.out.println("Bandwidth: " + kb / elapsedSecs + " kb/s");
    System.out.println("Bandwidth: " + bits / elapsedSecs / 1000 / 1000 + " Mbps");
    System.out.println("Latency: " + elapsedSecs / iterations + " s");
    stats.addBendwidthWrite(bits / elapsedSecs / 1000 / 1000);
    stats.addLatencyWrite(elapsedSecs / iterations);

    // Read
    ActiveReadableChannel readableChannel = proxy.getReadableChannel();

    time1 = System.currentTimeMillis();

    long totalRead = 0;
    int currentRead;
    for (int i = 0; i < iterations; i++) {
      buffer.clear();
      currentRead = readableChannel.read(buffer);
      if (currentRead > 0) {
        totalRead += currentRead;
      }
    }
    readableChannel.close();

    time2 = System.currentTimeMillis();

    elapsedSecs = (double) (time2 - time1) / 1000;
    kb = (double) totalRead / 1024;
    bits = (double) totalRead * 8;

    System.out.println();
    System.out.println("READ");
    System.out.println("Elapsed reading: " + elapsedSecs + " s");
    System.out.println("Bytes read: " + kb + " kb");
    System.out.println("Bandwidth: " + kb / elapsedSecs + " kb/s");
    System.out.println("Bandwidth: " + bits / elapsedSecs / 1000 / 1000 + " Mbps");
    System.out.println("Latency: " + elapsedSecs / iterations + " s");
    stats.addBendwidthRead(bits / elapsedSecs / 1000 / 1000);
    stats.addLatencyRead(elapsedSecs / iterations);

    // Clean up
    proxy.delete();
    try {
      store.delete(filename, true).get();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void actionAsync(String filename, int buffSize, int iterations, int batch) throws Exception {
    // Create
    CrailObject obj = store.create(filename, CrailNodeType.OBJECT, CrailStorageClass.DEFAULT,
        CrailLocationClass.DEFAULT, false)
        .get().asObject();
    CrailObjectProxy proxy = obj.getProxy();
    proxy.create(BwAction.class);

    ConcurrentLinkedQueue<ByteBuffer> bufferQueue = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < batch; i++) {
      ByteBuffer buffer = ByteBuffer.allocateDirect(buffSize);
      bufferQueue.add(buffer);
    }

    // Write
    LinkedBlockingQueue<Future<Integer>> futureQueue = new LinkedBlockingQueue<Future<Integer>>();
    HashMap<Integer, ByteBuffer> futureMap = new HashMap<Integer, ByteBuffer>();
    double ops = 0.0;
    ActiveAsyncChannel writableChannel = proxy.getWritableAsyncChannel();

    long time1 = System.currentTimeMillis();

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
    }
    long time2 = System.currentTimeMillis();
    writableChannel.close();

    double elapsedSecs = (double) (time2 - time1) / 1000;
    double kb = (double) buffSize * iterations / 1024;
    double bits = (double) buffSize * iterations * 8;

    System.out.println("Async WRITE");
    System.out.println("Elapsed writing: " + elapsedSecs + " s");
    System.out.println("Bytes written: " + kb + " kb");
    System.out.println("Bandwidth: " + kb / elapsedSecs + " kb/s");
    System.out.println("Bandwidth: " + bits / elapsedSecs / 1000 / 1000 + " Mbps");
    System.out.println("Latency: " + elapsedSecs / iterations + " s");
    stats.addBendwidthWrite(bits / elapsedSecs / 1000 / 1000);
    stats.addLatencyWrite(elapsedSecs / iterations);

    // Read
    bufferQueue.clear();
    for (int i = 0; i < batch; i++) {
      ByteBuffer buffer = ByteBuffer.allocateDirect(buffSize);
      bufferQueue.add(buffer);
    }

    double totalRead = 0.0;
    ops = 0;
    ActiveAsyncChannel readableChannel = proxy.getReadableAsyncChannel();
    futureMap.clear();
    futureQueue.clear();

    time1 = System.currentTimeMillis();
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
      totalRead += result;
    }
    while (!futureQueue.isEmpty()) {
      Future<Integer> future = futureQueue.poll();
      int result = future.get();
      futureMap.get(future.hashCode());
      totalRead += result;
    }
    time2 = System.currentTimeMillis();
    readableChannel.close();

    elapsedSecs = (double) (time2 - time1) / 1000;
    kb = (double) totalRead / 1024;
    bits = (double) totalRead * 8;

    System.out.println();
    System.out.println("Async READ");
    System.out.println("Elapsed reading: " + elapsedSecs + " s");
    System.out.println("Bytes read: " + kb + " kb");
    System.out.println("Bandwidth: " + kb / elapsedSecs + " kb/s");
    System.out.println("Bandwidth: " + bits / elapsedSecs / 1000 / 1000 + " Mbps");
    System.out.println("Latency: " + elapsedSecs / iterations + " s");
    stats.addBendwidthRead(bits / elapsedSecs / 1000 / 1000);
    stats.addLatencyRead(elapsedSecs / iterations);

    proxy.delete();
    try {
      store.delete(filename, true).get();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void close() throws Exception {
    store.close();
  }
}
