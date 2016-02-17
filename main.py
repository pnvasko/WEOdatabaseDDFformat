# -*- coding: utf-8 -*-


import os
import pandas as pd
import logging
import csv

BASE_DIR = os.path.dirname(os.path.abspath('__file__'))
DATA_DIR = BASE_DIR + "/data/"
FILE_NAME = 'WEOApr2015all.csv'
OUT_FILE_NAME = 'all.csv'


class Controller:
    def __init__(self):
        self.logger = logging.getLogger('Controller')
        self.logger.info("Start ...")
        file_name = BASE_DIR + "/" + FILE_NAME
        #self.df0 = pd.read_csv(file_name, sep='\t')
        self.df0 = self.load_input_file(file_name)
        out_file_name = BASE_DIR + "/" + OUT_FILE_NAME
        self.df0.to_csv(out_file_name, mode='w', header=True, index=False, encoding='utf-8', sep='|')
        self.country = self._get_country()
        self.subject_descriptor = self._get_subject_descriptor()
        self.geo_country = pd.read_csv(DATA_DIR + "/ddf--list--geo--country.csv", header=0, sep=',')

        self._get_slicer_by_sub_con()

    def _get_country_name_by_iso(self, iso):
        self.logger.info("start _get_country_name_by_iso %s ... " %iso)
        df0 = self.geo_country.query('geo == "%s"' %(iso.lower()))
        name = df0.head(1).name.values[0]

        return name

    def _get_slicer_by_sub_con(self):
        iso = self.country[0]
        df1 = self.df0.query('(ISO == "%s") & (WEOSubjectCode == "%s")' %(iso, self.subject_descriptor[0]))
        df1 = pd.DataFrame(df1, columns = ['1980', '1981', '1982', '1983', '1984', '1985', '1986', '1987', '1988', '1989',
                     '1990', '1991', '1992', '1993', '1994', '1995', '1996', '1997', '1998', '1999',
                     '2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009',
                     '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019',
                     '2020'])
        print(df1)
        df1 = df1.transpose()
        country_name = self._get_country_name_by_iso(iso)
        df1['geo'] = df1.apply(lambda x: self.country[0], axis=1)
        df1['geo.name'] = df1.apply(lambda x: country_name, axis=1)
        print(df1)


    def _get_country(self):
        self.logger.info("start _get_country ...")
        out_array = []
        g = self.df0.groupby(['ISO']).groups
        print("Country ISO: ", g)
        for country in g:
            out_array.append(country)
        return out_array

    def _get_subject_descriptor(self):
        self.logger.info("start _get_subject_descriptor ...")
        out_array = []
        g = self.df0.groupby('WEOSubjectCode').groups
        for subject in g:
            out_array.append(subject)
        return out_array

    def load_input_file(self, filename):
        self.logger.info("Load input file ...")
        csv_date = []
        i = 0
        with open(filename, "r") as file:
            reader = csv.reader(file, delimiter='\t')
            for row in reader:
                if i>0:
                    csv_date.append(row)
                i += 1
        if csv_date:
            headers = ['WEOCountryCode', 'ISO', 'WEOSubjectCode', 'Country', 'SubjectDescriptor',
                     'SubjectNotes', 'Units', 'Scale', 'CountrySeriesspecificNotes',
                     '1980', '1981', '1982', '1983', '1984', '1985', '1986', '1987', '1988', '1989',
                     '1990', '1991', '1992', '1993', '1994', '1995', '1996', '1997', '1998', '1999',
                     '2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009',
                     '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019',
                     '2020', 'EstimatesStartAfter']
            df = pd.DataFrame.from_records(csv_date, columns = headers)
            return df

    def finish(self):
        self.logger.info("Finish work ...")

def main():
    logging.basicConfig(level=logging.INFO,
                        format='[%(process)-5d] %(asctime)-15s [%(name)s] ''%(levelname)s: %(message)s')
    logging.getLogger('MAIN').setLevel(logging.INFO)
    logging.info("Start")
    file_name = BASE_DIR + "/" + FILE_NAME
    controler = Controller()
    controler.load_input_file(file_name)
    controler.finish()

if __name__ == "__main__":
    main()

