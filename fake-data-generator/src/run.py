from utils import logs_config as log
from utils.config_parser import ConfigParser
from app.fake_data import FakeData
from app.db import DatabaseManager
import argparse

conf = ConfigParser()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Fake Data Parameters.")
    parser.add_argument("--n_orders", type = int, default = 100, help = "Number of Orders.")
    parser.add_argument("--n_cust", type = int, default = 100, help = "Number of Customers in Day")
    args = parser.parse_args()
    log.LOG_MAIN.info("start main flow")
    fake = FakeData(args.n_orders, args.n_cust)
    log.LOG_GET.info(f"Generate Fake Orders Data, {fake.num_orders} will be generated")
    orders = fake.get_orders()
    customers = fake.get_cutomer_data()
    items = fake.get_items_data()
    db =  DatabaseManager(conf.creds["user"], conf.creds["password"], conf.creds["host"], conf.creds["db"])
    db.create_schema_if_not()
    db.create_orders_table()
    db.create_customer_table()
    db.create_items_table()
    db.insert_data(orders, customers, items)
    log.LOG_MAIN.info("main flow finished successfully")