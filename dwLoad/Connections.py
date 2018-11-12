#!/usr/bin/python
import psycopg2, MySQLdb, datetime


class Connections:

    def __init__(self):
        self.is_connected = True
        self.dw_host = ""
        self.dw_pass = ""
        self.dw_user = ""
        self.dw_name = ""

        connect_dw()

    def get_is_connected(self):
        if self.is_connected:
            print("DB Is Connected")
        else:
            print("DB is NOT Connected")

    def connect_dw(self):
        try:
            print str(datetime.datetime.now()) + " - Attempting to connect to the Data Warehouse...."
            psycopg2.connect(dbname=self.dw_name, user=self.dw_user, password=self.dw_pass,
                                                       host=self.dw_host,
                                                       port="5439")
            print str(datetime.datetime.now()) + " - Success!"
        except e as Exception:
            print (str(datetime.datetime.now()) + " - Failed to connect!")
