from urllib.request import urlopen
from shutil import copyfileobj
from os import path, remove
from pandas import read_csv, DataFrame
from datetime import datetime
from xlrd import open_workbook


class IndexConstituents:

    def __init__(self, symbol):
        self.symbol = symbol
        self.raw_file_outpath = ''

    def _get_index_url(self, symbol):
        # get the appropriate url for each index
        d = {
            'NDX': 'https://www.invesco.com/us/financial-products/etfs/holdings/main/holdings/0?ticker=QQQ&action=download',
            'SPX': 'https://www.ssga.com/us/en/individual/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx',
            'IND': 'https://www.ssga.com/us/en/individual/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-dia.xlsx'
        }

        # make sure the symbol is valid before trying to connect to website
        assert self.symbol in d.keys(), "{} is not a valid symbol. Must be {}".format(self.symbol,
                                                                                      list(d.keys())
                                                                                      )
        return d[symbol]

    def _get_file_extension(self, symbol):
        # map the symbols to the file extension type
        d = {'NDX': 'csv', 'SPX': 'xlsx', 'IND': 'xlsx'}
        # make sure the symbol is valid before trying to connect to website
        assert self.symbol in d.keys(), "{} is not a valid symbol. Must be {}".format(self.symbol,
                                                                                      list(d.keys()))
        return d[symbol]

    def download_index_file(self, outfile_dir):
        url = self._get_index_url(self.symbol)
        self.raw_file_outpath = path.join(outfile_dir, '{}_RAW.{}'.format(self.symbol,
                                                                             self._get_file_extension(self.symbol)))
        with urlopen(url) as response, open(self.raw_file_outpath, 'wb') as out_file:
            copyfileobj(response, out_file)

    def file_format_dataframe(self, raw_file_path=''):
        # initialize some variables
        symbols_supported = ['NDX', 'SPX', 'IND']
        raw_file_path = self.raw_file_outpath if self.raw_file_outpath != '' else raw_file_path
        renamed_cols = ['Company', 'Symbol', 'Sector', 'Weight', 'SharesHeld']
        file_last_update = ''

        # make sure the symbol I'm looking for is supported
        assert self.symbol in symbols_supported, \
            "{} not a supported symbol for file formatting. Use {}".format(self.symbol, symbols_supported)

        # make sure the a raw_file_path exists
        assert raw_file_path != '', "{} raw file path has not been designated. If download_index_file() has not been " \
                                    "run a path must be supplied as an input to raw_file_path.".format(self.symbol)

        # if symbol is NDX
        if self.symbol == symbols_supported[0]:
            cols_to_keep = ['Name', 'HoldingsTicker', 'Sector', 'Weight', 'Shares']  # want this specific order
            assert len(cols_to_keep) == len(renamed_cols), "Mismatch in length of column headers."

            df = read_csv(raw_file_path)
            # get and format last update date
            file_last_update = datetime.strftime(datetime.strptime(df.Date.iloc[0],
                                                                                     '%m/%d/%Y'), '%Y%m%d')

            # clean up column headers
            df = df[cols_to_keep]
            df.columns = renamed_cols
            # reindex to base 1
            df.index = range(1, len(df) + 1)
            df['LastUpdate'] = file_last_update
            # replace '.' with '/' for TOS symbols
            df['Symbol'] = df.Symbol.replace(r"\.", '/', regex=True)
            # remove unneeded spaces
            df['Symbol'] = df.Symbol.str.strip()
            return file_last_update, df

        # if symbol is SPX or IND, they have the same raw format
        elif self.symbol == symbols_supported[1] or self.symbol == symbols_supported[2]:
            workbook = open_workbook(raw_file_path)
            worksheet = workbook.sheet_by_index(0)
            df = DataFrame()
            row_in_df = False
            for i in range(worksheet.nrows):
                row_list = worksheet.row_values(i)

                # collect file refresh date
                if file_last_update == '' and "As of" in row_list[1]:
                    file_last_update = datetime.strftime(datetime.strptime(row_list[1].split(' ')[-1],
                                                                                             '%d-%b-%Y'), '%Y%m%d')

                # use row_in_df to tell what rows to use and what to ignore
                if row_in_df is True and row_list[:3] == ['', '', '']:
                    break

                if row_in_df and "CASH_USD" not in row_list:
                    company, symbol, sector, weight, shares = row_list[0], row_list[1], \
                                                              row_list[5], row_list[4], \
                                                              row_list[6]
                    df = df.append(dict(zip(renamed_cols, [company, symbol, sector, weight, shares])),
                                   ignore_index=True)

                # use row_in_df to tell what rows to use and what to ignore
                if row_in_df is False and row_list[0].strip() == "Name":
                    row_in_df = True

            # clean up column headers
            df = df[renamed_cols]
            # reindex to base 1
            df.index = range(1, len(df) + 1)
            df['LastUpdate'] = file_last_update
            # replace '.' with '/' for TOS symbols
            df['Symbol'] = df.Symbol.replace(r"\.", '/', regex=True)
            # remove unneeded spaces
            df['Symbol'] = df.Symbol.str.strip()
            return file_last_update, df

        else:
            raise AttributeError("No format rule in file_format_dataframe() for symbol {}".format(self.symbol))

    @staticmethod
    def write_df_to_csv(df, csv_outfile_path):
        df.to_csv(csv_outfile_path)

    def delete_raw_file(self):
        # This is only meant to be run in an automated process so it uses self.raw_file_outpath
        remove(self.raw_file_outpath)
