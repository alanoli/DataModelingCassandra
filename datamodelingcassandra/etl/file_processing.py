import pandas as pd
import re
import os
import glob
import json
import csv


class Pipeline:
    """Manages the processing of the input files and creating a unified file"""
    def __init__(self, data_path, output_file):
        self.data_path = data_path
        self.output_file = output_file
        self.full_data_rows_list = []

        self._process_files()
        self._write_output_file()

    def _process_files(self):
        """Reads through all files of the input directory and creates a list with data read inside each file"""
        for root, dirs, files in os.walk(self.data_path):
            file_path_list = glob.glob(os.path.join(root,'*'))
        
        for f in file_path_list:
            with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
                csvreader = csv.reader(csvfile) 
                next(csvreader)
                
                for line in csvreader:
                    self.full_data_rows_list.append(line) 

    def _write_output_file(self):
        """Creates a .csv output file with data read on _process_files function"""
        csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

        with open(self.output_file, 'w', encoding = 'utf8', newline='') as f:
            writer = csv.writer(f, dialect='myDialect')
            writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                        'level','location','sessionId','song','userId'])
            for row in self.full_data_rows_list:
                if (row[0] == ''):
                    continue
                writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))