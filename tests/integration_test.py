# run end to end csv to parquet conversion
from scripts import utils
import os
from scripts import constants
from scripts import preprocess_data
from scripts import csvtoparquetconverter as csv_master
import time


def test_csv_to_parquet(cleanup=True):
    start_time = time.time()
    print('################# Started End to End Test - test_csv_to_parquet ###################')
    # step 1 create Raw file
    sample_data = "1012001'~'CEC2017KOSU0053'~'2018-01-13 00:00:00'~'W.P.TRADING CORP'~''~'350 5th Avenue'~'New York'~'New York'~'10118'~'United States'~'44425805'~'KING OCEAN AGENCY INC.'~'Parque Industrial Machangara, Panamericana Norte km 4, Cuenca, Azuay, Ecuador'~''~'Cuenca'~'Azuay'~''~'Ecuador'~'33499873'~'GRAIMAN CIA. LTDA.'~'0190122271001'~''~'A Customs Brokerage, Inc.'~'GUAYAQUIL'~'PORT EVERGLADES'~'United States'~'UNITED STATES OF AMERICA'~''~''~''~''~''~''~'PLANET V'~'Antigua And Barbuda'~'AG'~'PORCELANA'~'31'~'PAQUETES'~'26770'~'STC GLAZED PORCELAIN'~'1'~'0'~''~'KING OCEAN ECUADOR KINGOCEAN S.A.'~'2018-03-20 00:00:00#@#@#"
    test_file_name = 'con_xyz_test_file'
    raw_file = constants.input_path + test_file_name
    row = 1000
    result = utils.create_dummy_file(sample_data, rows=row, output_file=raw_file)
    if not result:
        print('Step1 - Creation of Dummy File Failed')
        assert False

    # Step 2 clear out config files
    os.remove(constants.config_path)

    # step 3 start preprocessing the file
    if not preprocess_data.preprocess_file():
        print('Step3 - Pre-processing of file failed')
        assert False

    # step 4 convert to parquet
    if not csv_master.convert_to_pqt():
        print('Step4 - Conversion to pqt failed')
        assert False

    if cleanup:
        os.remove(constants.process_path + 'bk_' + test_file_name)
        os.remove(raw_file)

    print('################# Completed End to End Test - test_csv_to_parquet ###################')
    print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == '__main__':
    test_csv_to_parquet(True)