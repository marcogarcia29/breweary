import great_expectations as gx

context = gx.get_context()

datasource = context.sources.add_spark("my_spark_datasource")