import Connections


class DataPull:

    def __init__(self):
        self.complete = False
        self.db = Connections.Connections()
        self.db.get_is_connected()

    def start_data_pull(self, view, schema, table_name=""):
        print("Starting Data Pull: ")

        try:
            print("Connecting to DW...")
            self.db.connect_dw()
            print("Extracted Data")
            print(self.db.query_dw("select natural_key from %s.%s limit 50;" % (schema, view, )))
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
