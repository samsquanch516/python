import psycopg2, datetime


class Connections:

    def __init__(self):
        self.is_connected = True
        self.dw_host = ""
        self.dw_pass = ""
        self.dw_user = ""
        self.dw_name = ""
        self.dw_connection = self.connect_dw()

    def get_is_connected(self):
        return self.is_connected

    def connect_dw(self):
        try:
            print str(datetime.datetime.now()) + " - Attempting to connect to the Data Warehouse...."
            c = psycopg2.connect(dbname=self.dw_name, user=self.dw_user, password=self.dw_pass,
                                                       host=self.dw_host,
                                                       port="5439")
            print str(datetime.datetime.now()) + " - Success!"
            return c
        except Exception as e:
            print(str(datetime.datetime.now()) + " - Failed to connect!")
            print(e)

    def disconnect_dw(self):
        try:
            self.dw_connection.close()
        except Exception as e:
            print(e)

    def query_dw(self, query):
        if self.is_connected:
            cur = self.dw_connection.cursor()
            cur.execute(query)
            print(cur.fetchall())
        else:
            print("Not Connected to DW")
