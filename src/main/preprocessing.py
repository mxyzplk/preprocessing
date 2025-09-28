from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, expr, mean, greatest, least, lit, regexp_replace
import os
from functools import reduce

class Data:
    def __init__(self):
        self.data = None
        self.spark = SparkSession.builder.appName("DataLoader").getOrCreate()


    def read_csv(self, filename, col_names):
        main_dir = os.path.dirname(os.path.abspath(__file__))
        resources_dir = os.path.join(main_dir, '../resources')
        filepath = os.path.join(resources_dir, 'data', filename)

        self.filename = filename
        self.data = self.spark.read.csv(filepath, header=True, inferSchema=False)
        self.data = self.data.toDF(*[c.lower() for c in self.data.columns])

        self.data =self.data.select(col_names)    


    def substitute(self, col_name, input_values, output_values, data_type):
        if col_name in self.data.columns:
            
            self.data.select(col_name).show()

            self.data = self.data.withColumn(col_name, col(col_name).cast("string"))

            if len(input_values) == 1:
                if (output_values[0] == None):
                    expression = when(col(col_name) == str(input_values[0]), output_values[0]).otherwise(col(col_name))
                else:
                    expression = when(col(col_name) == str(input_values[0]), str(output_values[0])).otherwise(col(col_name))
            else:
                expression = reduce(
                    lambda acc, pair: when(col(col_name) == pair[0], lit(pair[1])).otherwise(acc),
                    zip(input_values[1:], output_values[1:]),
                    when(col(col_name) == input_values[0], lit(output_values[0]))
                )

            self.data = self.data.withColumn(col_name, expression)
            
            self.data = self.data.withColumn(col_name, col(col_name).cast(data_type))

            self.data.select(col_name).show()


    def calculate(self, col_name, method):
        if col_name in self.data.columns:

            self.data.select(col_name).show()
            self.data = self.data.withColumn(col_name, col(col_name).cast("float"))

            if method == "mean":
                value = self.data.select(mean(col_name)).collect()[0][0]
            
            elif method == "mode":
                value = self.data.groupBy(col_name).count().orderBy("count", ascending=False).first()[0]
                self.data.select(col_name).show()


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


    def remove_quotes(self, col_name):
        self.data = self.data.withColumn(col_name, col(col_name).cast("string"))

        self.data = self.data.withColumn(
            col_name,
            regexp_replace(col(col_name), r"^[\"'“”‘’]|[\"'“”‘’]$", "")
        )   
