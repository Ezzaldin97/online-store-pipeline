import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd
import time
from datetime import datetime
import os
from utils.chunkfy import Chunkfy
from utils import logs_config as log


class DatabaseManager:
    def __init__(self, user:str, password:str, host:str, db:str) -> None:
        """
        init function
        parameters
        ----------
        user:database username
        password:database password
        host:database IP address
        """
        self.user = user
        self.password = password
        self.host = host
        self.db = db
        
    def create_orders_table(self) -> None:
        log.LOG_DB.info(f"Create Orders Table in DB for {datetime.now().strftime('%Y%m%d')} data")
        query = f"""
        CREATE TABLE store.orders{datetime.now().strftime('%Y%m%d')}(
            cid INTEGER NOT NULL,
            item VARCHAR(15),
            created_at  DATE
        );
        """
        engine = create_engine(f"postgresql://{self.user}:{self.password}@{self.host}/{self.db}")
        with engine.connect() as conn:
            try:
                conn.execute(query)
            except:
                log.LOG_DB.info(f"Table Already Existed, Handling in progress")
                conn.execute(f"DROP TABLE store.orders{datetime.now().strftime('%Y%m%d')};")
                self.create_orders_table()
    
    def create_customer_table(self) -> None:
         log.LOG_DB.info(f"Create Customer Table in DB for {datetime.now().strftime('%Y%m%d')} data")
         query = f"""
         CREATE TABLE store.customers{datetime.now().strftime('%Y%m%d')}(
            cid INTEGER NOT NULL,
            dob DATE,
            country VARCHAR(5),
            address VARCHAR(200),
            gender VARCHAR(1),
            credit_card_provider VARCHAR(50),
            credit_card_number VARCHAR(100),
            credit_card_expire VARCHAR(10),
            credit_card_security_code VARCHAR(10)
        );
        """
         engine = create_engine(f"postgresql://{self.user}:{self.password}@{self.host}/{self.db}")
         with engine.connect() as conn:
            try:
                conn.execute(query)
            except:
                log.LOG_DB.info(f"Table Already Existed, Handling in progress")
                conn.execute(f"DROP TABLE store.customers{datetime.now().strftime('%Y%m%d')};")
                self.create_customer_table()
    def create_items_table(self) -> None:
        log.LOG_DB.info(f"Create Item Table in DB for {datetime.now().strftime('%Y%m%d')} data")
        query = f"""
         CREATE TABLE store.items{datetime.now().strftime('%Y%m%d')}(
            item VARCHAR(20) NOT NULL,
            price INTEGER,
            description VARCHAR(1000),
            discount_flag VARCHAR(1),
            last_day_sales INTEGER,
            last_day_review_score FLOAT
        );
        """
        engine = create_engine(f"postgresql://{self.user}:{self.password}@{self.host}/{self.db}")
        with engine.connect() as conn:
            try:
                conn.execute(query)
            except:
                log.LOG_DB.info(f"Table Already Existed, Handling in progress")
                conn.execute(f"DROP TABLE store.items{datetime.now().strftime('%Y%m%d')};")
                self.create_items_table()
    
    def create_schema_if_not(self) -> None:
        log.LOG_DB.info(f"Create Store Schema if Not Exist")
        query = f"""
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name = 'store';
        """
        engine = create_engine(f"postgresql://{self.user}:{self.password}@{self.host}/{self.db}")
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
            if df.shape[0] == 0:
                conn.execute("CREATE SCHEMA store;")
    
    def insert_data(self, orders_df:pd.DataFrame, customers_df:pd.DataFrame, items_df:pd.DataFrame) -> None:
        log.LOG_DB.info(f"Insert Data Records to DB")
        engine = create_engine(f"postgresql://{self.user}:{self.password}@{self.host}/{self.db}")
        with engine.connect() as conn:
            orders_df.to_sql(f"orders{datetime.now().strftime('%Y%m%d')}", conn, "store", if_exists="replace", index = False, chunksize = 10_000)
            customers_df.to_sql(f"customers{datetime.now().strftime('%Y%m%d')}", conn, "store", if_exists="replace", index = False, chunksize = 10_000)
            items_df.to_sql(f"items{datetime.now().strftime('%Y%m%d')}", conn, "store", if_exists="replace", index = False, chunksize = 10_000)
            
                
        