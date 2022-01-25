import pandas as pd
import json
import glob

class LoadInvoice:
    def __init__(self):
        print("Object created.")
        
    def read_invoices(self, year, month, day):
        print(year, month, day)
        
        list_data = []
        
        for file in glob.glob("plugins/invoices/d{}-{}-{}/*.json".format(str(year), str(month), str(day))):
            print(file)
            
            with open(file, 'r') as f:
                data = json.load(f)
                list_data.append(data)

        invoice_df = pd.DataFrame.from_records(list_data)
        print(invoice_df.shape[0])
        return invoice_df

    def get_sql(self, invoice_staging):
        insert_invoice_fact = """insert into invoice_fact
                                (date_sk, customer_sk, product_sk, supplier_sk, is_discount, total_qty, total_price)
                                select t.date_sk, t.customer_sk, t.product_sk, t.supplier_sk, sum(t.is_discount)/count(*) as is_discount, sum(t.qty)  as total_qty, sum(t.price) as total_price
                                from
                                (
                                    select * from {invoice_staging} as invoice
                                    left join customer_dim as customer
                                    on invoice.customer_id = customer.customer_id
                                    left join product_dim as product
                                    on invoice.product_id = product.product_id
                                    left join supplier_dim as supplier
                                    on invoice.supplier_id = supplier.supplier_id
                                    left join date_dim as date1
                                    on invoice.invoice_date = date1.full_date

                                ) as t
                                group by t.date_sk,t.customer_sk, t.product_sk, t.supplier_sk""".format(invoice_staging=invoice_staging)

        return insert_invoice_fact

    def del_sql(self, invoice_staging):
        return "drop table {invoice_staging}".format(invoice_staging = invoice_staging)

if __name__ == '__main__':
    sql = LoadInvoice().get_sql()
    print(sql)


