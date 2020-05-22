from argparse import ArgumentParser
from os import path, mkdir
from time import sleep
from index_constituents import IndexConstituents
from master_file import MasterFile
import logging


def download_etf_data(data_directory, index_list):
    for symbol in index_list:
        try:
            # use the SPY, QQQ, and DIA etfs to get constituents
            logging.info("Beginning ETF constituents download for {}".format(symbol))
            constituents = IndexConstituents(symbol)

            constituents.download_index_file(data_directory)
            logging.info("Downloaded raw etf file.")

            file_update, data_df = constituents.file_format_dataframe()
            logging.info("Successfully formatted file. Last updated {}".format(file_update))

            write_file_name = path.join(data_directory, "{}.csv".format(symbol))
            constituents.write_df_to_csv(data_df, write_file_name)
            logging.info('Wrote {} ETF data to "{}"'.format(symbol, write_file_name))
            constituents.delete_raw_file()
            logging.info('Deleted {} raw file'.format(symbol))
        except Exception as e:
            logging.exception(e)
        sleep(1)  # sleep a second to not hammer the etf websites
    logging.info('Make master_constituents.csv file')


def make_master_file(data_directory, index_list):
    mf = MasterFile(data_directory, index_list)
    file_destination_path = path.join(data_directory, 'master_constituents.csv')
    mf.make_master_dataframe().to_csv(file_destination_path, index=None)


if __name__ == '__main__':
    log_file = 'get_constituents.log'
    symbol_list = ['SPX', 'IND', 'NDX']

    # parse command line arguments
    arg_parser = ArgumentParser()
    arg_parser.add_argument(
        '-d',
        '--data_dir',
        help="Optional: the directory you want to output data to. If not supplied will use './data'. " +
             "If it doesn't exist it will be created."
    )
    args = arg_parser.parse_args()

    # assign the data_dir to the desired directory
    if args.data_dir is None:
        data_dir = 'data'
    else:
        data_dir = args.data_dir

    # if the data_dir doesn't exist create one
    if path.isdir(data_dir) is False:
        mkdir(data_dir)

    # set up logging, overwrite the file each time
    logging.basicConfig(filename=path.join(data_dir, log_file), level=logging.INFO,
                        format='%(asctime)s.%(msecs)03d %(message)s', datefmt='%Y%m%d %H:%M:%S',
                        filemode='w')

    logging.info("********** Beginning Get Constituents **********")

    # run methods
    download_etf_data(data_dir, symbol_list)
    make_master_file(data_dir, symbol_list)

    logging.info("********** Finished Get Constituents **********")