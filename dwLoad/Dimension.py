class Dimension(object):
    def __init__(self, view, schema, table_name, stage_table=""):
        self.view = view
        self.schema = schema
        self.destination_table = table_name
        self.stage_table = stage_table

    def get_view(self):
        return self.view

    def get_schema(self):
        return self.schema
