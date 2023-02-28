#!/usr/bin/env python

import sqlalchemy as sa
from sqlalchemy import Column, String, Integer, Float, DateTime, ForeignKey, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
import logging
from collections import deque
import pandas as pd
import sqlite3
import numpy as np
import argparse

## Note: To make this db more scalable, the preferred architecture would involve connecting to a MySql Server as opposed to a sqlite DB
## Exceptions and outliers for the dataset would include larger numbers as inputs into the tables, as well as strings/ missing values. 
## The user has the option to input a logging file for each data run as well as the input file name and whether the file is large or small for upload to the DB. 

Base = declarative_base()

engine = None
session = None

class SampleDB(Base):
    __tablename__ = 'main_table'

    field1 = Column(Integer, primary_key=True)
    field2 = Column(String, primary_key=True)
    field3 = Column(Integer)
    field4 = Column(Integer)
    field5 = Column(Integer)

def db_setup():

    global engine, session
    engine_str = 'sqlite:///sample_reporting.db'
    try:
        engine = create_engine(engine_str)
        Base.metadata.create_all(engine)
        session = sessionmaker(bind=engine)()
    except:
        logging.error("** Error Initializing DB **")
    return session

class loadCSV(Base):
    __tablename__ = 'main_table'

    def __init__(self, input_file=None, input_db_table_name=None, engine_str=None, input_log_file=None):
        input_file=self.input_file
        input_db_table_name=self.input_db_table_name
        engine_str=self.engine_str

    def update_table(df):
        EditColNames=['Field3', 'Field4', 'Field5']
        df[EditColNames]=None
        OldCols=['Field3_x','Field4_x','Field5_x']
        NewCols=['Field3_y','Field4_y','Field5_y']
        TargetColNames=['Field1', 'Field2', 'Field3', 'Field4', 'Field5', 'IUD']
        df_Insert=df.loc[df['IUD']=='I'].reset_index()
        df_Update=df.loc[df['IUD']=='U'].reset_index()
        df_Delete=df.loc[df['IUD']=='D'].reset_index()
        df_Insert[EditColNames]=df_Insert[NewCols]
        df_Update[EditColNames]=df_Update[NewCols]
        df_Delete[EditColNames]=df_Delete[OldCols]
        output=pd.concat([df_Insert[TargetColNames], \
        df_Update[TargetColNames], df_Delete[TargetColNames]] \
        ,axis=0).reset_index()
        updated_df=pd.DataFrame(output)[TargetColNames]
        updated_df[EditColNames]=updated_df[EditColNames].astype(int)
        return updated_df

    def load_merge_df(self):
        dbconnection = sqlite3.connect(self.engine_str)
        db_df = pd.read_sql_query("SELECT * from {}".format(self.input_db_table_name), dbconnection)
        load_df=pd.read_csv(self.input_file)
        if db_df.empty==False:
            merged_df=db_df.merge(load_df, how='outer', on=['Field1', 'Field2'])
            status_list=[]
            for i, row in merged_df.iterrows():
                if str(row['Field3_x'])==str(np.nan):
                    status_list.append('I')
                elif str(row['Field3_x'])!=str(np.nan) and str(row['Field3_y'])!=str(np.nan):
                    status_list.append('U')
                elif str(row['Field3_y'])==str(np.nan):
                    status_list.append('D')
            merged_df['IUD']=status_list
            loadCSV.update_table(merged_df).to_sql(self.input_db_table_name, dbconnection, if_exists='replace')
            return "Additional Table Merged with Existing DB"
        else:
            load_df['IUD']='I'
            load_df.to_sql(self.input_db_table_name, dbconnection, if_exists='replace')
            return "Initial Table Loaded into DB"

    ## Function to handle upload of large data files and for scaling up database /upload: Note this function will need to be further tested for larger datasets. 
    # Each dataframe chunk is appended directly to the sqlite table
    def process_chunk(self):
        dbconnection = sqlite3.connect(self.engine_str)
        db_df = pd.read_sql_query("SELECT * from {}".format(self.input_db_table_name), dbconnection)
        load_df=pd.read_csv(self.input_file)
        if db_df.empty==False:
            merged_df=db_df.merge(load_df, how='outer', on=['Field1', 'Field2'])
            status_list=[]
            for i, row in merged_df.iterrows():
                if str(row['Field3_x'])==str(np.nan):
                    status_list.append('I')
                elif str(row['Field3_x'])!=str(np.nan) and str(row['Field3_y'])!=str(np.nan):
                    status_list.append('U')
                elif str(row['Field3_y'])==str(np.nan):
                    status_list.append('D')
            merged_df['IUD']=status_list
            loadCSV.update_table(merged_df).to_sql(self.input_db_table_name, dbconnection, if_exists='append')
            return "Large Dataframe Chunk Merged with Existing DB"
    
    ## Function for loading large DF: Note this function would need to be further tested on edge cases / outliers
    def load_large_df(self):
        chunksize=1000
        with pd.read_csv(self.input_file, chunksize=chunksize) as reader:
            for chunk in reader:
                loadCSV.process(chunk)

class CommandLine(object):

    def __init__(self, csv_data, log_data, large_table):
        self.log_data = log_data
        self.csv_data = csv_data
        self.large_table = large_table

    def update(self):
        logging.basicConfig(filename=self.log_data, encoding='utf-8', level=logging.DEBUG)

        if self.csv_data:
            loadCSV.input_file=self.csv_data
            loadCSV.input_db_table_name=SampleDB.__tablename__
            loadCSV.engine_str='sample_reporting.db'
            load_data=loadCSV()
            logging.info('Loading {}'.format(self.csv_data))
            updated_data = load_data.load_merge_df()
            logging.info('** DB Updated {} **'.format(updated_data))
        
        elif self.large_table==True:
            loadCSV.load_large_df.input_file=self.csv_data
            loadCSV.load_large_df.input_db_table_name=SampleDB.__tablename__
            loadCSV.load_large_df.engine_str='sample_reporting.db'
            loadCSV.load_large_df()

    def query():
        dbconnection = sqlite3.connect(loadCSV.engine_str)
        output=pd.read_sql_query("SELECT * from {}".format(loadCSV.input_db_table_name), dbconnection)
        print(output)

def main():
    db_setup()
    parser = argparse.ArgumentParser()
    parser.add_argument('--csvupload', type=str, help='Filename for a csv to upload directly to DB',
                            default='', required=True)
    parser.add_argument('--logfilename', type=str, help='Filename for a logging text file',
                            default='', required=True)
    parser.add_argument('--largetable', type=str, help='Indicator variable for size of file. If file for upload exceeds 1 Mb, label as True. Otherwise, label this variable as False',
                            default='', required=True)
    args = parser.parse_args()
    logging.basicConfig(filename=args.logfilename, encoding='utf-8', level=logging.DEBUG)
    CommandInfo=CommandLine(args.csvupload, args.logfilename, args.largetable)
    CommandInfo.update()
    CommandLine.query()

if __name__ == '__main__':
    main()






