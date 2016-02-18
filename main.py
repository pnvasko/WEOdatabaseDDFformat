# -*- coding: utf-8 -*-


import os
import pandas as pd
import logging
import csv
import re


base_dir = os.path.dirname(os.path.abspath(__file__))
SETTINGS = {'BASE_DIR': base_dir,
            'DATA_DIR': base_dir + "/data/",
            'CSSN_DIR': 'country_series_specific_notes',
            'FILE_NAME': 'WEOApr2015all.csv',
            'GEO_COUNTRY': 'ddf--list--geo--country.csv',
            'OUT_FILE_NAME': 'all.csv',
            'DDF_SUBJECT': "ddf--weo--subject.csv",
            'DDF_Country_Series_Specific_Notes': ("ddf-weo--","--country--series--specific--notes.csv")
}

class Controller:
    def __init__(self):
        self.logger = logging.getLogger('Controller')
        self.logger.info("Start ...")
        self.settings = self._get_settings()
        self.ddffiles = []
        self._init_data_folders()
        self.geo_country = pd.read_csv(self.settings['DATA_DIR'] + "/" + self.settings['GEO_COUNTRY'], header=0, sep=',')
        self.df0 = self.load_input_file(self.settings['BASE_DIR'] + "/" + self.settings['FILE_NAME'])
        self.country = self._get_country()
        self.subject_descriptor = self._get_subject_descriptor()

    def start(self):
        self._make_weo_subject()
        self._get_slicer_by_sub_con()
        self._get_country_series_specific_notes()

    def _get_country_series_specific_notes(self):
        for subject in self.subject_descriptor:
            df1 = self.df0.query('(WEOSubjectCode == "%s")' % subject[0])
            df1 = pd.DataFrame(df1, columns=['WEOSubjectCode', 'ISO', 'CountrySeriesspecificNotes'])
            df1.columns = ['weo_subject_code', 'geo', 'country_series_specific_notes']
            file_name = "%s/%s/%s%s%s" \
                        % (self.settings['DATA_DIR'], self.settings['CSSN_DIR'],
                           self.settings['DDF_Country_Series_Specific_Notes'][0],
                           subject[0],
                           self.settings['DDF_Country_Series_Specific_Notes'][1])
            self.ddffiles.append([file_name, "weo_subject_descriptor_country_series_specific_notes"])
            df1.to_csv(file_name, mode='w', header=True, index=False, encoding='utf-8', sep=',')

    def _get_estimates_start_by_country_subject(self):
# weo subject code    geo     country-estimates-start-after
        pass

    def _get_slicer_subject_country(self):
# geo    geo.name     year    weo-subject-descriptor
        pass

    def _get_subject_name_by_code(self, code):
        pass

    def _get_slicer_by_sub_con(self):
        iso = self.country[3]
        code = self.subject_descriptor[11][0]

        df1 = self.df0.query('(ISO == "%s") & (WEOSubjectCode == "%s")' % (iso, code))
        df1 = pd.DataFrame(df1, columns=['1980', '1981', '1982', '1983', '1984', '1985', '1986', '1987', '1988',
                                         '1989', '1990', '1991', '1992', '1993', '1994', '1995', '1996', '1997',
                                         '1998', '1999', '2000', '2001', '2002', '2003', '2004', '2005', '2006',
                                         '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015',
                                         '2016', '2017', '2018', '2019', '2020'])
        df1 = df1.transpose()
        country_name = self._get_country_name_by_iso(iso)
        df1['geo'] = df1.apply(lambda x: iso, axis=1)
        df1['geo.name'] = df1.apply(lambda x: country_name, axis=1)
        df1.reset_index(level=0, inplace=True)
        df1.columns = ['year', self.subject_descriptor[0][0], 'geo', 'geo.name']
        print(df1[0:2])

    def _make_weo_subject(self):
        self.logger.info("start _make_weo_subject ...")
        out_array = []
        g = self.df0.groupby(['WEOSubjectCode', 'SubjectDescriptor', 'SubjectNotes', 'Units', 'Scale']).groups
        for item in g:
            out_array.append(item)
        headers = ['weo_subject_code', 'subject_descriptor', 'subject_notes', 'units', 'scale']
        df2 = pd.DataFrame.from_records(out_array, columns=headers)
        self.ddffiles.append([self.settings['DDF_SUBJECT'], "weo_subject_descriptor"])
        df2.to_csv(self.settings['DATA_DIR'] + "/" + self.settings['DDF_SUBJECT'], mode='w', header=True, index=False, encoding='utf-8', sep=',')
        return out_array

    def _get_country_name_by_iso(self, iso):
        self.logger.info("start _get_country_name_by_iso %s ... " %iso)
        df0 = self.geo_country.query('geo == "%s"' %(iso.lower()))
        name = df0.head(1).name.values[0]
        return name

    def _get_country(self):
        self.logger.info("start _get_country ...")
        out_array = []
        g = self.df0.groupby(['ISO']).groups
        for country in g:
            out_array.append(country)
        return out_array

    def _get_subject_descriptor(self):
        self.logger.info("start _get_subject_descriptor ...")
        out_array = []
        g = self.df0.groupby(['WEOSubjectCode', 'SubjectDescriptor']).groups
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


    def _init_data_folders(self):
        if not os.path.exists(self.settings['DATA_DIR']):
            if not os.path.isdir(self.settings['DATA_DIR']):
                os.mkdir(self.settings['DATA_DIR'])
        if not os.path.exists(self.settings['DATA_DIR'] +"/" + self.settings['CSSN_DIR']):
            if not os.path.isdir(self.settings['DATA_DIR'] +"/" + self.settings['CSSN_DIR']):
                os.mkdir(self.settings['DATA_DIR'] +"/" + self.settings['CSSN_DIR'])


    def _get_settings(self):
        return SETTINGS

    def finish(self):
        self.logger.info("Finish work ...")

def main():
    logging.basicConfig(level=logging.INFO,
                        format='[%(process)-5d] %(asctime)-15s [%(name)s] ''%(levelname)s: %(message)s')
    logging.getLogger('MAIN').setLevel(logging.INFO)
    logging.info("Start")
    controler = Controller()
    controler.start()
    controler.finish()

if __name__ == "__main__":
    main()

