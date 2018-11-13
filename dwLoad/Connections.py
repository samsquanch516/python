import psycopg2, datetime, MySQLdb


class Connections:

    def __init__(self):
        self.is_connected_dw = False
        self.is_connected_db = False
        self.dw_host = ""
        self.dw_pass = ""
        self.dw_user = ""
        self.dw_name = "reporting"

        self.db_host = "vox"
        self.db_pass = ""
        self.db_user = ""
        self.db_name = "reporting"

        self.dw_connection = self.connect_dw()
        self.db_connection = self.connect_db()

    def get_is_dw_connected(self):
        return self.is_connected_dw

    def get_is_db_connected(self):
        return self.is_connected_db

    def connect_dw(self):
        try:
            print str(datetime.datetime.now()) + " - Attempting to connect to the Data Warehouse...."
            c = psycopg2.connect(dbname=self.dw_name, user=self.dw_user, password=self.dw_pass,
                                                       host=self.dw_host,
                                                       port="5439")
            print str(datetime.datetime.now()) + " - Success!"
            self.is_connected_dw = True
            return c
        except Exception as e:
            print(str(datetime.datetime.now()) + " - Failed to connect!")
            print(e)

    def connect_db(self):
        # Trying to connect
        try:
            print str(datetime.datetime.now()) + " - Attempting to connect to the Database...."
            d = MySQLdb.connect(self.db_host, self.db_user, self.db_pass, self.db_name)
            print str(datetime.datetime.now()) + " - Success!"
            self.is_connected_db = True
            return d
        except Exception as e:
            print("Can't connect to database!")
            print(e)

    def disconnect_dw(self):
        try:
            self.dw_connection.close()
            self.is_connected_dw = False
            print("Disconnected from DW")
        except Exception as e:
            print(e)

    def disconnect_db(self):
        try:
            self.db_connection.close()
            self.is_connected_db = False
            print("Disconnected from DB")
        except Exception as e:
            print(e)

    def query_dw(self, query):
        if self.is_connected_dw:
            cur = self.dw_connection.cursor()
            cur.execute(query)
            print(cur.fetchone())
        else:
            print("Not Connected to DW")

    def query_db(self, query):
        if self.is_connected_db:
            cursor = self.db_connection.cursor()
            cursor.execute(query)
            print(cursor.fetchone())
        else:
            print("Not Connected to DW")
