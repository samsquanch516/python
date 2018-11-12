import time, Connections


class DataPull:

    def __init__(self):
        self.complete = False
        self.db = Connections.Connections()
        self.db.get_is_connected()

    def start_data_pull(self, loading):
        print("Starting Data Pull: " + loading)
        print("Extracted Data")
        print("Loaded Data " + loading)
        print(self.db.query_dw())
        try:
            self.db.disconnect_dw()
            print("Disconnected from dw")
        except Exception as e:
            print(e)
        return self.set_complete(True)

    def set_complete(self, is_complete):
        self.complete = is_complete
        print("Load Complete")
