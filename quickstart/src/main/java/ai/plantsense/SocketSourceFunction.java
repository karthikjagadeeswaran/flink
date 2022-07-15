package ai.plantsense;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import ai.plantsense.*;

public class SocketSourceFunction extends RichSourceFunction<RowData> implements ResultTypeQueryable<RowData> {

  private final String path;
  private final DeserializationSchema<RowData> deserializer;

  private volatile boolean isRunning = true;
  private String currentFile = null;

  public SocketSourceFunction(String path, DeserializationSchema<RowData> deserializer) {
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
    while (isRunning) {
      // open and consume from socket
      try {
        List<String> filenames = new LinkedList<String>();

        final File[] folder = new File(path).listFiles();
        Arrays.sort(folder, Comparator.comparingLong(File::lastModified));

        for (final File fileEntry : folder) {
          if (fileEntry.getName().contains(".csv") && currentFile==null){
            currentFile = fileEntry.getName();
          }
        }
        File initialFile = new File(path+"/"+currentFile);
        InputStream targetStream = new FileInputStream(initialFile);

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int b;
        while ((b = targetStream.read()) >= 0) {
          // buffer until delimiter
//           if (b != byteDelimiter) {
//            buffer.write(b);
//           }
          // decode and emit record
//           else {
             ctx.collect(deserializer.deserialize(buffer.toByteArray()));
             buffer.reset();
//           }
        }
      } catch (Throwable t) {
        t.printStackTrace(); // print and continue
      }
      Thread.sleep(1000);
    }
  }

  @Override
  public void cancel() {
    isRunning = false;
    try {
//      currentFile.close();
    } catch (Throwable t) {
      // ignore
    }
  }
}