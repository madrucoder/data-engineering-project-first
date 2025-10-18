from pyspark.sql import SparkSession

def read_postgress(spark):
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://159.223.187.110:5432/novadrive") \
        .option("dbtable", "yourtable") \
        .option("user", "etlreadonly") \
        .option("password", "novadrive376A@") \
        .load()
    return df


def read_excel(spark, file_path):
    df = spark.read.format("com.crealytics.spark.excel") \
        .option("header", "true") \
        .load(file_path)
    return df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("DataConnector").getOrCreate()

    postgres_df = read_postgres(spark)

    excel_df = read_excel(spark, "data/dados.xlsx")

    # Save data to MINio
    postgres_df.write.parquet("s3a://your-bucket/landing/postgres")
    excel_df.write.parquet("s3a://your-bucket/landing/excel")