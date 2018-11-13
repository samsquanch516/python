import Connections


class DataPull:

    def __init__(self):
        self.complete = False
        self.db = Connections.Connections()

    def start_data_pull(self, view, schema, table_name=""):
        print("Starting Data Pull: ")
        print(view)

        try:
            if self.db.get_is_db_connected():
                print("Connected to DB")
            else:
                print("Not connected to DB")

            if self.db.get_is_dw_connected():
                print("Connected to DW")
            else:
                print("Not connected to DW")

            try:
                # pull data
                print("Extracting Data...")
                if self.db.get_is_dw_connected():
                    print("Connected to DW")
                else:
                    print("Not connected to DW")
                print(self.db.query_dw("select natural_key from %s.%s limit 50;" % (schema, view, )))
            except Exception as e:
                print(e)

            print("Loaded Data ")

            try:
                self.db.disconnect_dw()
                print("Disconnected from dw")
            except Exception as e:
                print(e)
            return self.set_complete(True)
        except Exception as e:
            print(e)
            print("Data Pull Failed!")

    def set_complete(self, is_complete):
        self.complete = is_complete
        print("Load Complete")
