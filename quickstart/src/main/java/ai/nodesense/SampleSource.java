package ai.nodesense;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

public class SampleSource extends RichSourceFunction<RowData> {
  @Override
  public void run(SourceContext<RowData> ctx) throws Exception {
      ctx.collect(GenericRowData.of(
          StringData.fromString("Subject 1"),
          StringData.fromString("Hello, World!")
      ));
  }

  @Override
  public void cancel(){}
}
