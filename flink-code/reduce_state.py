from functools import reduce
from pyflink.common import Row
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, ReduceFunction, RuntimeContext
from pyflink.datastream.state import ReducingStateDescriptor

class ReduceState(ReduceFunction):

    def __init__(self):
        self.sum = None
        print('initial')

    def open(self, runtime_context: RuntimeContext):
        descriptor = ReducingStateDescriptor(
            name="sum",  # the state name
            reduce_function=self.reduce,
            type_info=Types.PICKLED_BYTE_ARRAY()  # type information
        )
        self.sum = runtime_context.get_reducing_state(descriptor)
        print('open')

    def reduce(self, value1, value2):
        print("initial values:",value1, value2)
        # access the state value
        current_sum = self.sum.get() 
        print("initial current sum:",current_sum)
        if current_sum is None:
            current_sum = value1[0]

        if value2 is None:
            current_sum = value1[0]
        else:
            current_sum = value1[0]+value2[0]

        # update the state
        self.sum.add((current_sum, value1[1]))
        print("after current sum:",self.sum.get(),current_sum)
        if value1[1]!=value2[1]:
            return self.sum.get()

    def close(self):
        self.sum.clear()
        print("close",self.sum.get())


env = StreamExecutionEnvironment.get_execution_environment()
# ds.key_by(lambda x: x[1]).reduce(lambda a, b: a[0] + b[0], b[1])
env.from_collection([(1, 'a'), (2, 'a'), (3, 'a'), (4, 'b')]) \
    .key_by(lambda row: row[1]) \
    .reduce(ReduceState()) \
    .print()

env.execute()

# the printed output will be (1,4) and (1,5)