package ai.nodesense;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;

public class SampleTableSource implements ScanTableSource {
  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext ctx) {
    boolean bounded = true;
    final SampleSource source = new SampleSource();
    return SourceFunctionProvider.of(source, bounded);
  }

  @Override
  public DynamicTableSource copy() {
    return new SampleTableSource();
  }

  @Override
  public String asSummaryString() {
    return "IMAP Table Source";
  }
}
