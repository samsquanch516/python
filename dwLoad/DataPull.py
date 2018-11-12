import time, Connections


class DataPull:

    def __init__(self):
        self.complete = False
        db = Connections.Connections()
        db.get_is_connected()

    def start_data_pull(self, loading, sleep_time):
        print("Starting Data Pull: " + loading + " at "+str(sleep_time))
        print("Extracted Data")
        print("Loaded Data " + loading)
        time.sleep(sleep_time)
        return self.set_complete(True)

    def set_complete(self, is_complete):
        self.complete = is_complete
        print("Load Complete")
