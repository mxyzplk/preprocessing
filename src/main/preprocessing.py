from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, expr, mean, greatest, least
import os

class Data:
    def __init__(self):
        self.data = None
        self.spark = SparkSession.builder.appName("DataLoader").getOrCreate()


    def read_csv(self, filename, col_names):
        main_dir = os.path.dirname(os.path.abspath(__file__))
        resources_dir = os.path.join(main_dir, '../resources')
        filepath = os.path.join(resources_dir, 'data', filename)

        self.filename = filename
        self.data = self.spark.read.csv(filepath, header=True, inferSchema=True)
        self.data = self.data.toDF(*[c.lower() for c in self.data.columns])

        self.data =self.data.select(col_names)    


    def substitute(self, col_name, input_values, output_values):
        if col_name in self.data.columns:

            expression = when(col(col_name) == input_values[0], output_values[0])

            if len(input_values) > 1:
                for inp, out in zip(input_values[1:], output_values[1:]):
                    expression = expression.when(col(col_name) == inp, out)      

            expression = expr.otherwise(col(col_name))         

            self.data = self.data.withColumn(col_name, expression)


    def calculate(self, col_name, input_values, method):
        if col_name in self.data.columns:
            
            # non-float values must be None
            expression = when(col(col_name) == input_values[0], None)

            if len(input_values) > 1:
                for inp in input_values[1:]:
                    expression = expression.when(col(col_name) == inp, None)      

            # for operations, values must be float
            expression = expression.otherwise(col(col_name)).cast("float")         

            self.data = self.data.withColumn(col_name, expression)

            if method == "mean":
                value = self.data.select(mean(col_name)).collect()[0][0]
            
            elif method == "mode":
                value = self.data.groupBy(col_name).count().orderBy("count", ascending=False).first()[0]

            self.data = self.data.fillna({col_name: value})


    def create_column(self, new_column, col_names, method):
        if method == "sum_columns":
            expr_val = sum([col(c) for c in col_names])
        elif method == "mean_columns":
            expr_val = sum([col(c) for c in col_names]) / len(col_names)
        elif method == "max_columns":
            expr_val = greatest(*[col(c) for c in col_names])
        elif method == "min_columns":
            expr_val = least(*[col(c) for c in col_names])
        else:
            raise NotImplementedError(f"Method {method} not_implemented")            

        self.data = self.data.withColumn(new_column, expr_val)
    

    def create_column_custom(self, new_column, expression):
        expr_val = expr(expression)
        self.data = self.data.withColumn(new_column, expr_val)

