import logging.handlers
import sys
import datetime
import calendar
from monthdelta import monthdelta
import numpy as np
import csv
import re
import time
import urllib2



        #self.df0 = self._load_data_from_file(file_name)

    def _load_data_from_file(self, file_name):
#        df = pd.read_excel(file_name)
        df = pd.read_csv(file_name, sep='\t')
        print(file_name)

        return df