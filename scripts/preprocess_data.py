from multiprocessing.dummy import Pool as ThreadPool
import os
import time
import utils

# below config can be passed as input parameters
# for POC just passing these as constant hard coded
threads = 6
input_path = 'C:\\Users\\sanchit.latawa\\Desktop\\csv_parquet\\incoming_data\\'
output_path = 'C:\\Users\\sanchit.latawa\\Desktop\\csv_parquet\\processed\\'

file_to_be_migrated = [(input_path + file, output_path + file, '(\'~\')', '#@#@#', 18000) if file.startswith("con")  else '' for file in os.listdir(input_path)]
print(file_to_be_migrated)

if __name__ == '__main__':
    start_time = time.time()
    pool = ThreadPool(int(threads))
    print(pool.starmap(utils.massage_data, file_to_be_migrated))
    pool.close()
    pool.join()
    print("--- %s seconds ---" % (time.time() - start_time))
    print('---------- Completed: Processing ----------------')
