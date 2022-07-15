package ai.nodesense.customSource;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import java.io.*;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class CustomSourceFunction extends RichSourceFunction<RowData> implements ResultTypeQueryable<RowData> {

  private final String path;
  private final DeserializationSchema<RowData> deserializer;

  private volatile boolean isRunning = true;
  private File currentFile;

  public CustomSourceFunction(String path, DeserializationSchema<RowData> deserializer) {
    this.path = path;
    this.deserializer = deserializer;
  }

  @Override
  public TypeInformation<RowData> getProducedType() {
    return deserializer.getProducedType();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
//    deserializer.open(() -> getRuntimeContext().getMetricGroup());
  }

  @Override
  public void run(SourceContext<RowData> ctx) throws Exception {
    List<String> filelist = new ArrayList<String>(0);
    while (isRunning) {
      // open and consume from socket
      currentFile = new File(this.path);
      if (currentFile.isDirectory()) {
        for (final File fileEntry : currentFile.listFiles()) {
          String fileName = fileEntry.getName();
          if(filelist.contains(fileName)){
            System.out.println("File Already Readed!!!");
          }
          else{
            try (FileReader fr = new FileReader(new File(this.path+"/"+fileName))) {
              BufferedReader br = new BufferedReader(fr);
              String line = " ";
              String[] tempArr;

              while ((line = br.readLine()) != null) {
                tempArr = line.split(",");
                List<Object> t = new ArrayList <Object>(tempArr.length);
                for (int i = 0; i <= tempArr.length - 1; i++) {
                  if(i==0){
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("[yyyy-MM-dd HH:mm:ss.SSSZZZZZ][yyyy-MM-dd HH:mm:ssZZZZZ][yyyy-MM-dd HH:mm:ss.SZZZZZ][yyyy-MM-dd HH:mm:ss.SSZZZZZ]");
                    t.add(TimestampData.fromLocalDateTime(LocalDateTime.parse(tempArr[i], formatter)));
                  }else {
                    t.add(StringData.fromString(tempArr[i]));
                  }
                }
                ctx.collect((GenericRowData.of(t.toArray())));
              }
            }
            catch (Throwable t) {
              t.printStackTrace(); // print and continue
            }
            filelist.add(fileName);
          }
        }
      }
      else {
        try (FileReader fr = new FileReader(currentFile)) {
          BufferedReader br = new BufferedReader(fr);
          String line = " ";
          String[] tempArr;

          while ((line = br.readLine()) != null) {
            tempArr = line.split(",");
            StringData[] t = new StringData[tempArr.length];
            for (int i = 0; i <= tempArr.length - 1; i++) {
              t[i] = StringData.fromString(tempArr[i]);
            }
            ctx.collect((GenericRowData.of(t)));
          }
        }
        catch (Throwable t) {
          t.printStackTrace(); // print and continue
        }
      }
      Thread.sleep(1000);
    }
  }

  @Override
  public void cancel() {
    isRunning = false;
//    try {
//      buffer.close();
//    } catch (Throwable t) {
//      // ignore
//      t.printStackTrace();
//    }
  }
}