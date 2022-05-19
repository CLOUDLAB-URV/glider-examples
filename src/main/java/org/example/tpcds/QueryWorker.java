package org.example.tpcds;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.parquet.example.data.Group;
import org.example.tpcds.parquet.Parquet;
import org.example.tpcds.parquet.Reader;

/* 
 * QUERY 15 ::
 * select ca_zip, sum(cs_sales_price)
 * from catalog_sales, customer, customer_address, date_dim
 * where cs_bill_customer_sk = c_customer_sk
 *    and c_current_addr_sk = ca_address_sk
 *    and ( substr(ca_zip,1,5) in ('85669', '86197','88274','83405','86475',
 *                                 '85392', '85460', '80348', '81792')
 *          or ca_state in ('CA','WA','GA')
 *          or cs_sales_price > 500)
 *    and cs_sold_date_sk = d_date_sk
 *    and d_qoy = 2 and d_year = 2001
 * group by ca_zip
 * order by ca_zip
 * limit 100
 * 
 */
public class QueryWorker implements Callable<Map<String, Double>> {
  private static final String CS_SALES_PRICE = "cs_sales_price";
  private static final String CS_BILL_CUSTOMER = "cs_bill_customer_sk";
  private static final String CS_SOLD_DATE = "cs_sold_date_sk";
  private static final String C_KEY = "c_customer_sk";
  private static final String C_CURRENT_ADDR = "c_current_addr_sk";
  private static final String CA_KEY = "ca_address_sk";
  private static final String CA_ZIP = "ca_zip";
  private static final String CA_STATE = "ca_state";
  private static final String D_KEY = "d_date_sk";
  private static final String D_QOY = "d_qoy";
  private static final String D_YEAR = "d_year";

  private static final List<String> zips = Arrays.asList("85669", "86197", "88274", "83405",
      "86475", "85392", "85460", "80348", "81792");
  private static final List<String> states = Arrays.asList("CA", "WA", "GA");

  private static String getStringOrEmpty(Group group, String fieldName) {
    try {
      return group.getBinary(fieldName, 0).toStringUsingUTF8();
    } catch (Exception e) {
      return "";
    }
  }

  private static final String CATALOG_SALES_FILE_PATH = "catalog_sales/";
  private static final String CUSTOMER_FILE_PATH = "customer/";
  private static final String CUSTOMER_ADDRESS_FILE_PATH = "customer_address/";
  private static final String DATE_DIM_FILE_PATH = "date_dim/";

  private String catalogSalesFile;
  private String customerFile;
  private String customerAddressFile;
  private String dateDimFile;

  /**
   * Worker for query 15.
   * <p>
   * The worker processes data from the given partitions. Only one partition per
   * table. And provides the partial result.
   * 
   * @param catalogSalesPart    Partition number for catalog sales table.
   * @param customerPart        Partition number for customer table.
   * @param customerAddressPart Partition number for customer address table.
   * @param dateDimPart         Partition number for data dim table.
   * @param baseDir             Base directory fro TPC-DS dataset.
   * @throws IOException Error locating the table files.
   */
  public QueryWorker(
      int catalogSalesPart, int customerPart,
      int customerAddressPart, int dateDimPart,
      String baseDir) throws IOException {

    catalogSalesFile = getFilePath(baseDir, CATALOG_SALES_FILE_PATH, catalogSalesPart);
    customerFile = getFilePath(baseDir, CUSTOMER_FILE_PATH, customerPart);
    customerAddressFile = getFilePath(baseDir, CUSTOMER_ADDRESS_FILE_PATH, customerAddressPart);
    dateDimFile = getFilePath(baseDir, DATE_DIM_FILE_PATH, dateDimPart);
  }

  private static String getFilePath(String baseDir, String tablePath, int partNum)
      throws IOException {
    try (Stream<java.nio.file.Path> files = Files.list(Paths.get(baseDir, tablePath))) {
      Optional<java.nio.file.Path> op = files
          .filter(f -> f.getFileName().toString()
              .startsWith(String.format("part-%05d-", partNum)))
          .findFirst();
      if (op.isPresent()) {
        return op.get().toString();
      } else {
        throw new IOException("Could not find file in directory.");
      }
    }
  }

  @Override
  public Map<String, Double> call() throws Exception {
    // ca_zip -> sum(cs_sales_price)
    final Map<String, Double> result = new HashMap<>();

    final Parquet customerTable = Reader.getParquetData(customerFile, C_KEY, C_KEY, C_CURRENT_ADDR);
    final Parquet customerAddressTable = Reader.getParquetData(customerAddressFile, CA_KEY,
        CA_KEY, CA_ZIP, CA_STATE);
    final Parquet dateDimTable = Reader.getParquetData(dateDimFile, D_KEY, D_KEY, D_QOY, D_YEAR);

    Reader.runPerRow(catalogSalesFile, sale -> {
      try {
        sale.getInteger(CS_SALES_PRICE, 0);
        sale.getInteger(CS_BILL_CUSTOMER, 0);
        sale.getInteger(CS_SOLD_DATE, 0);
      } catch (Exception e) {
        return;
      }
      Group customer = customerTable.getData()
          .get(sale.getInteger(CS_BILL_CUSTOMER, 0));
      if (customer == null) {
        return;
      }
      Group addr = customerAddressTable.getData()
          .get(customer.getInteger(C_CURRENT_ADDR, 0));
      if (addr == null) {
        return;
      }
      if (zips.contains(getStringOrEmpty(addr, CA_ZIP))
          || states.contains(getStringOrEmpty(addr, CA_STATE))
          || sale.getInteger(CS_SALES_PRICE, 0) > 50000) {
        Group date = dateDimTable.getData().get(sale.getInteger(CS_SOLD_DATE, 0));
        if (date == null) {
          return;
        }
        if (date.getInteger(D_QOY, 0) == 2
            && date.getInteger(D_YEAR, 0) == 2001) {
          String zip = getStringOrEmpty(addr, CA_ZIP);
          Integer p = sale.getInteger(CS_SALES_PRICE, 0);
          double price = p.doubleValue() / 100;

          result.merge(zip, price, Double::sum);
        }
      }
    }, CS_SALES_PRICE, CS_BILL_CUSTOMER, CS_SOLD_DATE);

    return result.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .limit(100)
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            Map.Entry::getValue,
            (e1, e2) -> e1,
            LinkedHashMap::new));
  }
}
