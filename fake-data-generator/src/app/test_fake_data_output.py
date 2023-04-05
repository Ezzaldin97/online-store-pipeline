import pytest
from fake_data import FakeData


@pytest.fixture
def fake():
    return FakeData()

def test_default_length_orders_data_generator(fake):
    actual = len(fake.get_orders())
    expected = 100    
    assert(actual==expected)
    
def test_default_length_customers_data_generator(fake):
    actual = len(fake.get_cutomer_data())
    expected = 100    
    assert(actual==expected)
    
def test_default_length_items_data_generator(fake):
    actual = len(fake.get_items_data())
    expected = len(fake.items)    
    assert(actual==expected)
    
def test_default_length_items_data_generator_column_names(fake):
    df = fake.get_items_data()
    actual = df.columns.tolist()
    expected = ["item", "price", "description", "discount_flag", "last_day_sales", "last_day_score"]
    assert(actual==expected)

