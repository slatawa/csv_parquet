from pyspark.sql import SparkSession
import utils
import os

# below config can be passed as input parameters
# for POC just passing these as constant hard coded

input_path = 'C:\\Users\\sanchit.latawa\\Desktop\\csv_parquet\\processed\\'
output_path = 'C:\\Users\\sanchit.latawa\\Desktop\\csv_parquet\\parquet\\'


def convert_csv_parquet():
    for file in os.listdir(input_path):
        try:
            sc = SparkSession.builder.master('local[*]') \
                 .config("spark.driver.memory", "15g").appName('test').getOrCreate()
            print(file)
            df_csv = utils.read_csv(sc, input_path + file)
            print(df_csv.count())
            utils.convertcsv_toparquet(df_csv, output_path+file)
        except:
           utils.print_exception('convert_csv_parquet')


if __name__ == '__main__':
    convert_csv_parquet()

