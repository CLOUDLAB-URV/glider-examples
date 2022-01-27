package org.example.wordcount;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.crail.CrailBufferedInputStream;
import org.apache.crail.CrailBufferedOutputStream;
import org.apache.crail.CrailFile;
import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNode;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailObjectProxy;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.CrailStore;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.conf.CrailConstants;

/**
 * Distributed WorCount computation in three steps:
 *  <ul>
 *    <li> Data is filtered for lines that contain a certain word
 *    <li> Filtered lines are processed in parallel to generate a wordcount dict
 *    <li> All dictionaries are merged into one
 *  </ul>
 * <p>
 * The initial dataset is split into different files: one per worker.
 * <p>
 * 2 implementations:
 * {@link DistributedWordCount#workersReduceLocal()} and
 * {@link DistributedWordCount#workersReduceCrail()}.
 */
public class DistributedWordCount {
  public static final String LOCAL_FILE = "/Datasets/wiki1/AA/wiki_%02d";
  public static final String MERGER_PATH = "/words";
  public static final int N_WORKERS = 10;

  private final CrailStore store;
  // actionPath -> filter action proxy
  private final Map<String, CrailObjectProxy> actionProxies = new ConcurrentHashMap<>();
  // actionPath -> action data size
  private final Map<String, Integer> dataSizes = new ConcurrentHashMap<>();
  // Workers
  private final ExecutorService es;
  private final ConcurrentLinkedQueue<Path> tasks = new ConcurrentLinkedQueue<>();

  public DistributedWordCount() throws Exception {
    CrailConfiguration conf = CrailConfiguration.createConfigurationFromFile();
    store = CrailStore.newInstance(conf);
    es = Executors.newFixedThreadPool(N_WORKERS);
  }

  public static void main(String[] args) {
    try {
      DistributedWordCount dis = new DistributedWordCount();
      dis.runMain();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Count the words in a stream of text lines.
   *
   * @param lines Stream of text lines to process
   * @return A Map with the counts of each word that appeared in the stream
   */
  public static Map<String, Long> countWords(Stream<String> lines) {
    return lines
        .flatMap(line ->
            Stream.of(line.toLowerCase().split("\\W+"))
                .filter(w -> !w.isEmpty()))
        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
  }

  /**
   * Create the crail node on <code>actionPath</code> as a <code>FilterAction</code>
   * and write to it the contents of the <code>localFile</code>.
   * <p>
   * If the node already exists, retrieve its proxy and size.
   * <p>
   * Saves the proxy to the filter action and its data size to the local maps.
   *
   * @param actionPath Path for the crail node (FilterAction).
   * @param localFile  Path of the local file to upload to the crail action.
   */
  private void createOrGetFilterAction(String actionPath, Path localFile) throws Exception {
    CrailNode crailNode = store.lookup(actionPath).get();
    CrailObjectProxy filterAction;
    int dataSize;
    if (crailNode == null) {
      crailNode = store.create(actionPath, CrailNodeType.OBJECT, CrailStorageClass.DEFAULT,
          CrailLocationClass.DEFAULT, false).get();
      filterAction = crailNode.asObject().getProxy();
      filterAction.create(FilterAction.class);

      byte[] bytes = Files.readAllBytes(localFile);
      ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
      buffer.put(bytes);
      buffer.rewind();
      dataSize = filterAction.write(buffer.array());
      setCrailFileSize(actionPath, dataSize);
    } else {
      filterAction = crailNode.asObject().getProxy();
      dataSize = getCrailFileSize(actionPath);
    }
    actionProxies.put(actionPath, filterAction);
    dataSizes.put(actionPath, dataSize);
  }

  private int getCrailFileSize(String crailPath) throws Exception {
    String sizePath = crailPath + "-size";
    CrailFile sizeFile = store.lookup(sizePath).get().asFile();
    CrailBufferedInputStream inputStream = sizeFile.getBufferedInputStream(Integer.SIZE);
    int size = inputStream.readInt();
    inputStream.close();
    return size;
  }

  private void setCrailFileSize(String crailPath, int dataSize) throws Exception {
    String sizePath = crailPath + "-size";
    try {
      store.delete(sizePath, true).get();
    } catch (Exception e) {
      e.printStackTrace();
    }
    CrailFile sizeFile = store.create(sizePath, CrailNodeType.DATAFILE, CrailStorageClass.get(1),
        CrailLocationClass.DEFAULT, false).get().asFile();
    CrailBufferedOutputStream outputStream = sizeFile.getBufferedOutputStream(Integer.SIZE);
    outputStream.writeInt(dataSize);
    outputStream.close();
  }

  /**
   * Run the computation on parallel workers (reading from FilterAction crail
   * objects) and reduce their results (maps) locally.
   *
   * @return The final aggregated Map with the word counts of all data.
   */
  private Map<String, Long> workersReduceLocal() {
    Callable<Map<String, Long>> workerCode = () -> {
      Path localFile = tasks.poll();
      if (localFile != null) {
        String actionPath = "/" + localFile.getFileName().toString();
        try {
          createOrGetFilterAction(actionPath, localFile);
          CrailObjectProxy filterAction = actionProxies.get(actionPath);
          int dataSize = dataSizes.get(actionPath);

          byte[] input = new byte[dataSize];
          filterAction.read(input);
          ByteArrayInputStream is = new ByteArrayInputStream(input);
          Stream<String> lines = new BufferedReader(new InputStreamReader(is)).lines();

          return countWords(lines);

        } catch (Exception e) {
          System.out.println("Error accessing crail.");
          e.printStackTrace();
        }
      }
      return new HashMap<>();
    };

    List<Future<Map<String, Long>>> futures = new ArrayList<>(N_WORKERS);
    for (int i = 0; i < N_WORKERS; i++) {
      futures.add(es.submit(workerCode));
    }
    List<Map<String, Long>> wordMaps = new ArrayList<>(N_WORKERS);
    for (Future<Map<String, Long>> future : futures) {
      try {
        wordMaps.add(future.get());
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
        wordMaps.add(new HashMap<>());
      }
    }
    // Reduce word counts
    return wordMaps.stream()
        .flatMap(m -> m.entrySet().stream())
        .collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.summingLong(Map.Entry::getValue)));
  }

  /**
   * Run the computation on parallel workers (reading from FilterAction crail
   * objects) and the workers send their results to a MapMerger crail object.
   * The merger aggregates maps as the workers send them.
   *
   * @return The final aggregated Map with the word counts of all data,
   * retrieved from the MapMerger object.
   */
  private Map<String, Long> workersReduceCrail() {
    Runnable workerCode = () -> {
      Path localFile = tasks.poll();
      if (localFile != null) {
        String actionPath = "/" + localFile.getFileName().toString();
        try {
          createOrGetFilterAction(actionPath, localFile);
          CrailObjectProxy filterAction = actionProxies.get(actionPath);
          int dataSize = dataSizes.get(actionPath);

          byte[] input = new byte[dataSize];
          filterAction.read(input);
          ByteArrayInputStream is = new ByteArrayInputStream(input);
          Stream<String> lines = new BufferedReader(new InputStreamReader(is)).lines();

          Map<String, Long> words = countWords(lines);
          CrailObjectProxy mapMerger = store.lookup(MERGER_PATH).get().asObject().getProxy();

          ByteArrayOutputStream byteOS = new ByteArrayOutputStream();
          ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteOS);
          objectOutputStream.writeObject(words);
          // write is synchronous: the merge is finished when the worker ends
          int write = mapMerger.write(byteOS.toByteArray());
          System.out.println("Map sent was " + write + " bytes");

        } catch (Exception e) {
          System.out.println("Error accessing crail.");
          e.printStackTrace();
        }
      }
    };

    // Create merger action
    try {
      CrailNode crailNode = store.create(MERGER_PATH, CrailNodeType.OBJECT, CrailStorageClass.DEFAULT,
          CrailLocationClass.DEFAULT, false).get();
      CrailObjectProxy merger = crailNode.syncDir().asObject().getProxy();
      merger.create(MapMerger.class);

      List<Future<?>> futures = new ArrayList<>(N_WORKERS);
      for (int i = 0; i < N_WORKERS; i++) {
        futures.add(es.submit(workerCode));
      }
      for (Future<?> future : futures) {
        try {
          future.get();
        } catch (InterruptedException | ExecutionException e) {
          e.printStackTrace();
        }
      }
      // Get reduced map from crail
      byte[] bytes = new byte[CrailConstants.BUFFER_SIZE];
      merger.read(bytes);
      ByteArrayInputStream byteIS = new ByteArrayInputStream(bytes);
      ObjectInputStream inputStream = new ObjectInputStream(byteIS);
      @SuppressWarnings("unchecked")
      Map<String, Long> words = (Map<String, Long>) inputStream.readObject();
      merger.delete();
      try {
        store.delete(MERGER_PATH, false).get();
        // BROKEN clientside: removes from datanode but client seeks negative position
      } catch (Exception e) {
        System.out.println(e);
//        e.printStackTrace();
      }
      return words;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return new HashMap<>();
  }

  private void runMain() {
    for (int i = 0; i < N_WORKERS; i++) {
      tasks.add(Paths.get(String.format(LOCAL_FILE, i)));
    }

    long time1 = System.currentTimeMillis();
//    Map<String, Long> words = workersReduceLocal();
    Map<String, Long> words = workersReduceCrail();
    long time2 = System.currentTimeMillis();

    // Show top 10
    List<Map.Entry<String, Long>> top10 = words.entrySet().stream()
        .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
        .limit(10)
        .collect(Collectors.toList());
    System.out.println(top10);

    System.out.println("Elapsed: " + (time2 - time1) + " ms");


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
}
