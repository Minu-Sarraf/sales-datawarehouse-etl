import pandas as pd
import json
import glob

class LoadInvoice:
    def __init__(self):
        print("Object created.")
        
    def read_invoices(self, date):
        print(date)
        list_data = []
        for file in glob.glob("invoices/{}/*.json".format('d'+ date)):
            with open(file, 'r') as f:
                data = json.load(f)
                list_data.append(data)
        invoice_df = pd.DataFrame.from_records(list_data)
        return invoice_df



