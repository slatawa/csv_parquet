import time
import re
import traceback
import multiprocessing
import os
import json


def read_csv(sparkSession, input_file):
    return sparkSession.read.csv(input_file, sep=',', inferSchema=True)


def read_file(config_path):
    with open(config_path, 'r') as file:
        return json.load(file)


def write_file(config_path, metadata):
    with open(config_path, 'w') as file:
        file.write(json.dumps(metadata))


def convertcsv_toparquet(df, output_folder):
    df = df.toDF(*(c.replace('_c', 'column') for c in df.columns))
    df.write.option("header", "true").mode("overwrite").parquet(output_folder)


def print_exception(function_name):
    print(f'Exception in function name - {function_name}')
    traceback.print_exc()


def create_dummy_file(sample_data, rows=100000, output_file='stress_test_data.txt'):
    # 10 gb creation - 20 seconds for rec in range(0,500000)
    # 18 gb creation - 43 seconds for rec in range(0,900000)
    try:
        print('---------- Start: create_dummy_file ----------------')
        # argument validation
        if sample_data == '':
            print('Empty input data passed, nothing to do - Exit')

        start_time = time.time()

        file = open(output_file, "w", encoding='utf-8')
        for rec in range(0, rows):
            # sampleData= sampleData.encode('UTF-8')
            file.write(sample_data)
        file.close()
        print("--- %s seconds ---" % (time.time() - start_time))
        print('---------- Complete: create_dummy_file ----------------')
        return True
    except:
        print_exception(create_dummy_file)
        raise


def read_in_chunks(file_object, chunk_size=18000):
    """Lazy function to read the file."""
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data


def massage_data(input_file, output_file, rec_sep, line_sep, chunk_size):
    '''
    Massge data is replacement utility function
    pass in custom record, line delimiter and input file
    the function will create file with standard rec(,) and line
    delimiter (newline)
    :param input_file: input file on which replacement needs to be applied
    :param output_file: output file post replacement
    :param rec_sep: column delimiter
    :param line_sep: line delimiter
    :param chunk_size: Size of file in kbs to read at one time
    :return: Boolean
    '''
    try:
        print('---------- Start: massage_data ----------------')
        start_time = time.time()
        # output_file = './/processed//' + os.path.basename(input_file)
        file = open(output_file, "w")

        with open(input_file) as f:
            for rec in read_in_chunks(f, chunk_size):
                rec = re.sub(rec_sep, ',', rec.replace(line_sep, '\n'))
                file.write(rec)

        file.close()

        print("--- %s seconds ---" % (time.time() - start_time))
        print('---------- Complete: massage_data ----------------')
        return True
    except:
        print_exception(massage_data)
        raise


