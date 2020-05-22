import os
import pandas as pd
import datetime


class MasterFile:
    def __init__(self, working_directory, index_list):
        self.working_directory = working_directory
        self.index_list = index_list

    def master_header_list(self):
        # make final header list
        master_header = []
        for item in self.index_list:
            master_header.append(f'{item}_Count')
            master_header.append(item)
        return ['All_Count', 'All'] + master_header + ['Last_Update']

    def index_map_and_all_symbol_list(self):
        # make map of index symbols {index: constituent_list}
        symbol_map = {}
        all_symbols_list = []
        for index in self.index_list:
            df = pd.read_csv(os.path.join(self.working_directory, f'{index}.csv'))
            symbol_list = list(df.Symbol)
            all_symbols_list += symbol_list
            symbol_map[index] = symbol_list
        return symbol_map, sorted(list(set(all_symbols_list)))

    def make_master_dataframe(self):
        symbol_map, all_symbols_list = self.index_map_and_all_symbol_list()
        self.index_map_and_all_symbol_list()
        n_rows = len(all_symbols_list)
        all_symbols_count = range(1, len(all_symbols_list) + 1)
        date_string = datetime.datetime.strftime(datetime.datetime.now(), "%m/%d/%Y %H:%M:%S")
        last_update_list = [date_string] + [''] * (n_rows - 1)

        master_df = pd.DataFrame({'All_Count': all_symbols_count,
                                  'All': all_symbols_list,
                                  'Last_Update': last_update_list})
        for index, symbol_list in symbol_map.items():
            # make a symbol count column
            symbols_count = list(range(1, len(symbol_list) + 1))
            # create blanks to fill in the empty cells in the df
            n_symbols = len(symbol_list)
            blank_list = [''] * (n_rows - n_symbols)
            # add cols to df
            master_df[f'{index}_Count'] = symbols_count + blank_list
            master_df[index] = sorted(symbol_list) + blank_list
        return master_df[self.master_header_list()]


