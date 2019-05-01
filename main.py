#!/usr/bin/env python3

import argparse
import os
import csv 
import operator
import random
import json
import sqlite3

from zipfile import ZipFile

# validate we have files 
def validate_input(args):

    if os.path.isdir(args.d):
        files = os.listdir(args.d)
        if files is None:
            print("No files in specified dir, not continueing:", args.d)
            raise SystemExit      

    if os.path.exists(args.o):
        print("DB already exists, not continueing:", args.o)
        raise SystemExit

# delete all txt files from input dir of data
def delete_text_files(dir_path):
    for i in os.listdir(dir_path):
        if "txt" in i:
            os.remove(dir_path + i)

# return files to use 
def get_file_list(dir_path):
    types = ["AMEX", "NASDAQ", "NYSE", "OTCBB"]
    all_files = os.listdir(args.d)
    return_files = []
    for i in all_files:
        if any(s in i for s in types):
            return_files.append(dir_path + i)

    return return_files

# unzip zip file
def unzip(f_path, dir_path):

    with ZipFile(f_path, 'r') as zipObj:
        zipObj.extractall(dir_path)

def put_into_db(dir_path, sql_path):
    conn = sqlite3.connect(sql_path)
    c = conn.cursor()

    sql_create_equities_table = """ CREATE TABLE IF NOT EXISTS Equities (
                                    ticker text NOT NULL,
                                    exchange text NOT NULL,
                                    date text NOT NULL,
                                    open real NOT NULL,
                                    high real NOT NULL,
                                    low real NOT NULL,
                                    close real NOT NULL,
                                    volume integer NOT NULL
                                ); """


    try:
        c.execute(sql_create_equities_table)
    except Exception as e:
        print(e)

    if dir_path is not None:
        txt_files = os.listdir(dir_path)
        for i in txt_files:
            if "txt" in i[-3:]:
                data = []
                e = i.split('_')[0]
                with open(dir_path+i, 'r') as f:
                    reader = csv.reader(f)
                    # skip headers
                    next(reader)
                    for row in reader:
                        t = row[0]
                        d = row[1][0:4] + " " + row[1][4:6] + " "+ row[1][6:8] 
                        o = row[2]
                        h = row[3]
                        l = row[4]
                        close = row[5]
                        v = row[6]
                        data.append( (t,e,d,o,h,l,close,v) )
            try:
                data_entry_string = """INSERT INTO Equities (ticker, exchange, date, open, high, low, close, volume) 
                                       VALUES (?,?,?,?,?,?,?,?)"""
                c.executemany(data_entry_string, data)
            except Exception as e:
                print(e)
            conn.commit()

    conn.close()



def main(args):
    validate_input(args)
    delete_text_files(args.d)
    files = get_file_list(args.d)
    for i in files:
        unzip(i, args.d)
    put_into_db(args.d, args.o)


if __name__ == "__main__":

    # Create arg parse
    parser = argparse.ArgumentParser(description='Open all zip files provided from EODData and put them into a sqlite db')
    parser.add_argument('-d', metavar='d', type=str, help='Provide input direcorty where all .zip files from EODData live', default="./data/")
    parser.add_argument('-o', metavar='k', type=str, help='Output file to use, the sqlite database that will be created, default is ./g.db', default="./data.db")

    # Parse Args
    args = parser.parse_args()

    main(args)
