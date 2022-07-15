from pyflink.common import Row
from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col
from pyflink.table.udf import AggregateFunction, udaf

class RounHour(AggregateFunction):

    def get_value(self, accumulator):
        return Row(accumulator[0], accumulator[1])

    def create_accumulator(self):
        return Row(0,0)

    def accumulate(self, accumulator, row):
        accumulator[0] += 1
        accumulator[1] += row.b

    def retract(self, accumulator, row):
        accumulator[0] -= 1
        accumulator[1] -= row.b

    def merge(self, accumulator, accumulators):
        for other_acc in accumulators:
            accumulator[0] += other_acc[0]
            accumulator[1] += other_acc[1]

    def get_accumulator_type(self):
        return DataTypes.ROW(
            [DataTypes.FIELD("a", DataTypes.BIGINT()),
             DataTypes.FIELD("b", DataTypes.BIGINT())])

    def get_result_type(self):
        return DataTypes.ROW(
            [DataTypes.FIELD("a", DataTypes.BIGINT()),
             DataTypes.FIELD("b", DataTypes.BIGINT())])