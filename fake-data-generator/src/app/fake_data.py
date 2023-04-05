"""
Main Module for Fake Data Generator.
"""
import random
import time
from datetime import datetime
from typing import Any, List, Tuple
import pandas as pd
from faker import Faker

random.seed(random.randint(1, 100))
Faker.seed(random.randint(1, 100))

class FakeData:
    def __init__(self, num_orders:int=100, num_cust_ids:int=100) -> None:
        """
        init function
        parameters
        ----------
        >> num_orders: number of orders
        """
        self.items = ["chair","car","toy","laptop","box","food","shirt","weights","bags","carts"]
        self.country = ["EGY", "IT", "US", "FRA", "RUS", "KSA", "UK", "JPN", "CH"]
        self.num_orders = num_orders
        self.cust_ids = [random.randint(1, 1_000_000) for _ in range(num_cust_ids)]
        self.fake = Faker()
    
    def get_orders(self) -> pd.DataFrame:
        """
        create Fake orders data for customers, order item and datetime of order created.
        """
        data_lst = []
        for order in range(self.num_orders):
            cid = random.choice(self.cust_ids)
            item = self.fake.random_element(elements = self.items)
            date = self.fake.date_between(start_date='-3d', end_date='-1d')
            data_lst.append([cid, item, date])
        df = pd.DataFrame(data_lst)
        df.columns = ["customer_id", "item", "created_at"]
        return df
    
    def get_cutomer_data(self) -> pd.DataFrame:
        """
        create fake customer profile data 
        """
        data_lst = []
        for id in self.cust_ids:
            dob = self.fake.date_between(start_date='-60y', end_date='-20y')
            country =self.fake.random_element(elements = self.country)
            address = self.fake.address()
            gender = self.fake.random_element(elements = ["M", "F"])
            credit_card_provider = self.fake.credit_card_provider()
            credit_card_number = self.fake.credit_card_number()
            credit_card_expire = self.fake.credit_card_expire()
            credit_card_security_code = self.fake.credit_card_security_code()
            data_lst.append([id, dob, country, address, gender, credit_card_provider, credit_card_number, credit_card_expire, credit_card_security_code])
        data = pd.DataFrame(data_lst)
        data.columns = ["id", "Date of Birth", "country", "address",
                        "gender", "credit_card_provider",
                        "credit_card_number", "credit_card_expire",
                        "credit_card_security_code"]
        return data
    
    def get_items_data(self) -> pd.DataFrame:
        """
        create fake item review data
        """
        data_lst = []
        for item in self.items:
            discount = self.fake.random_element(elements = ["Y", "N"])
            price = self.fake.random_number(digits=2, fix_len=3)
            description = self.fake.text()
            last_day_sales = self.fake.random_number(digits=3, fix_len=3)
            score = random.normalvariate(3, 0.52)
            data_lst.append([item, price, description, discount, last_day_sales, score])
        df = pd.DataFrame(data_lst)
        df.columns = ["item", "price", "description", "discount_flag", "last_day_sales", "last_day_score"]
        return df