class Connections:

    def __init__(self):
        self.is_connected = True

    def get_is_connected(self):
        if self.is_connected:
            print("DB Is Connected")
        else:
            print("DB is NOT Connected")

