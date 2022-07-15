from pyflink.common import WatermarkStrategy, Row
from pyflink.common.serialization import Encoder
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FileSink, OutputFileConfig, NumberSequenceSource
from pyflink.datastream.functions import RuntimeContext, MapFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.serialization import JsonRowDeserializationSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer,FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema, SerializationSchema,JsonRowSerializationSchema,Encoder
from pyflink.common.typeinfo import Types
import json
class MyMapFunction(MapFunction):

    def open(self, runtime_context):
        state_desc = ValueStateDescriptor('cnt', Types.DOUBLE())
        self.cnt_state = runtime_context.get_state(state_desc)

    def map(self, value):
        cnt = self.cnt_state.value()
        print("cnt: ",type(cnt),type(value[1]),cnt==value[1])
        if cnt is None:
            self.cnt_state.update(value[1])
            return Row(value[0],"First Time came")
        if cnt == value[1]:
            self.cnt_state.update(value[1])
            return Row(value[0],"same as prev")
        else:
            self.cnt_state.update(value[1])
            return  Row(value[0],"Updated")

def state_access_demo():
  

    env = StreamExecutionEnvironment.get_execution_environment()
    # the sql connector for kafka is used here as it's a fat jar and could avoid dependency issues
    # env.add_jars("file:///home/karthik/work/testdir/flink-1.15.0/lib/flink-sql-connector-kafka-1.15.0.jar")
    # env.add_python_archive("venv.zip")
   # specify the path of the python interpreter which is used to execute the python UDF workers
    env.get_config().set_python_executable("/home/karthik/work/testdir/pyflink/bin/python")
    deserialization_schema = JsonRowDeserializationSchema.builder().type_info(
                             type_info=Types.ROW_NAMED(
                             ["tag","value"], [Types.STRING(), Types.DOUBLE()])).build()

    ##Data: {"abc": "123", "xyz": "ddd"}


    kafka_consumer = FlinkKafkaConsumer(
        topics='test_source_topic',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'})

    ds = env.add_source(kafka_consumer)
    # ds.print()
    ds = ds.key_by(lambda a:a[0]).map(MyMapFunction(),output_type=Types.ROW([Types.STRING(), Types.STRING()]))
    # ds.print()


    serialization_schema = JsonRowSerializationSchema.builder().with_type_info(
        type_info=Types.ROW_NAMED(["tag","Desc"],[Types.STRING(), Types.STRING()])).build()

    kafka_producer = FlinkKafkaProducer(
        topic='test_sink_topic',
        serialization_schema=serialization_schema,
        producer_config={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'})

    ds.add_sink(kafka_producer)
    
    env.execute('state_access_demo')


if __name__ == '__main__':
    state_access_demo()

    

