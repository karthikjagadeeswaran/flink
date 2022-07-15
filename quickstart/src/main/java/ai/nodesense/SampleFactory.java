package ai.nodesense;

import java.util.HashSet;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

public class SampleFactory implements DynamicTableSourceFactory {
  @Override
  public String factoryIdentifier() {
    return "sample";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return new HashSet<>();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return new HashSet<>();
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context ctx) {
    final FactoryUtil.TableFactoryHelper factoryHelper = FactoryUtil.createTableFactoryHelper(this, ctx);
    factoryHelper.validate();

    return new SampleTableSource();
  }
}
