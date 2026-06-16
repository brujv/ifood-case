import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, TimestampType, DoubleType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NYCTaxiSilverPipeline:
    def __init__(self, spark: SparkSession, bronze_path: str, target_table: str):
        self.spark = spark
        self.bronze_path = bronze_path
        self.target_table = target_table
        
        self.required_columns = [
            F.col("VendorID").cast(IntegerType()),
            F.col("passenger_count").cast(IntegerType()),
            F.col("total_amount").cast(DoubleType()),
            F.col("tpep_pickup_datetime").cast(TimestampType()),
            F.col("tpep_dropoff_datetime").cast(TimestampType())
        ]

    def read_bronze(self) -> DataFrame:
        try:
            logger.info(f"Iniciando leitura dos arquivos no caminho: {self.bronze_path}")
            months = ["01", "02", "03", "04", "05"]
            df_list = []
            
            for month in months:
                df_temp = self.spark.read.parquet(f"{self.bronze_path}/yellow_tripdata_2023-{month}.parquet")
                df_list.append(df_temp)
            
            df_result = df_list[0]
            for df in df_list[1:]:
                df_result = df_result.unionAll(df)
            
            return df_result
        except Exception as e:
            logger.error(f"Erro crítico ao ler dados da camada Bronze: {str(e)}")
            raise

    def transform(self, df: DataFrame) -> DataFrame:
        logger.info("Iniciando transformações e aplicação de regras de Data Quality.")
        return (
            df.select(self.required_columns)
            .filter(
                F.col("tpep_pickup_datetime").isNotNull() &
                (F.col("tpep_dropoff_datetime") >= F.col("tpep_pickup_datetime")) &
                (F.col("total_amount") >= 0.0) &
                (F.col("passenger_count") > 0) &
                F.col("tpep_pickup_datetime").between("2023-01-01 00:00:00", "2023-05-31 23:59:59")
            )
            .withColumn("pickup_year_month", F.date_format("tpep_pickup_datetime", "yyyy-MM"))
        )

    def write_silver(self, df: DataFrame) -> None:
        try:
            logger.info(f"Gravando dados na tabela Delta: {self.target_table}")
            (
                df.write
                .format("delta")
                .mode("overwrite")
                .partitionBy("pickup_year_month")
                .option("overwriteSchema", "true")
                .saveAsTable(self.target_table)
            )
            logger.info("Camada Silver gravada com sucesso.")
        except Exception as e:
            logger.error(f"Falha na escrita da tabela Delta: {str(e)}")
            raise

    def run(self) -> None:
        df_raw = self.read_bronze()
        df_transformed = self.transform(df_raw)
        self.write_silver(df_transformed)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("NYCTaxiSilverPipeline").getOrCreate()
    
    spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.nyc_taxi_bronze")
    
    BRONZE_DIR = "/Volumes/workspace/default/nyc_taxi_bronze"
    TARGET_DELTA_TABLE = "workspace.default.nyc_taxi_silver"

    pipeline = NYCTaxiSilverPipeline(
        spark=spark, 
        bronze_path=BRONZE_DIR, \
        target_table=TARGET_DELTA_TABLE
    )
    pipeline.run()