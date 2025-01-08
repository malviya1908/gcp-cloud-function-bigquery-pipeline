import os
import random
import pandas as pd
from faker import Faker
from google.cloud import storage
from datetime import datetime

# Initialize Faker for generating fake data
fake = Faker()

# List of possible product categories and products
categories = ['Electronics', 'Clothing', 'Home Appliances', 'Books', 'Sports']
products = {
    'Electronics': ['Phone', 'Laptop', 'Headphones', 'Smartwatch'],
    'Clothing': ['T-shirt', 'Jeans', 'Sweater', 'Jacket'],
    'Home Appliances': ['Refrigerator', 'Washing Machine', 'Microwave', 'Vacuum Cleaner'],
    'Books': ['Novel', 'Biography', 'Science Fiction', 'Fantasy'],
    'Sports': ['Football', 'Basketball', 'Tennis Racket', 'Yoga Mat']
}

# Payment methods
payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Cash', 'Gift Card']

# Generate Retail Dataset
retail_data = []
for transaction_id in range(1, 101):
    customer_id = random.randint(1, 50)
    category = random.choice(categories)
    product = random.choice(products[category])
    quantity = random.randint(1, 5)
    price = random.uniform(5.99, 499.99)
    total = round(price * quantity, 2)
    date = fake.date_this_year()
    payment_method = random.choice(payment_methods)
    retail_data.append([transaction_id, customer_id, product, category, quantity, round(price, 2), total, date, payment_method])

# Create Retail DataFrame
retail_df = pd.DataFrame(retail_data, columns=["Transaction_ID", "Customer_ID", "Product", "Category", "Quantity", "Price", "Total", "Date", "Payment_Method"])

# Generate Customer Dataset
customer_data = []
for customer_id in range(1, 51):
    first_name = fake.first_name()
    last_name = fake.last_name()
    email = fake.email()
    phone = fake.phone_number()
    address = fake.address().replace("\n", ", ")
    state = fake.state()
    country = "USA"
    customer_data.append([customer_id, first_name, last_name, email, phone, address, state, country])

# Create Customer DataFrame
customer_df = pd.DataFrame(customer_data, columns=["Customer_ID", "First_Name", "Last_Name", "Email", "Phone", "Address", "State", "Country"])

# Set up authentication using the service account JSON key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"path_to_your_secrets_key_file\secrets_file.json"

# Get today's date in the format YYYY-MM-DD
today_date = datetime.today().strftime("%Y-%m-%d")

# GCS bucket path
bucket_name = "your_bucket_name"
retail_path = f"retail_data/retail/{today_date}/retail.csv"
customer_path = f"retail_data/customer/{today_date}/customer.csv"

# Initialize the GCS client
client = storage.Client()

# Create the GCS bucket object
bucket = client.get_bucket(bucket_name)

# Function to upload DataFrame to GCS as CSV
def upload_to_gcs(df, gcs_path):
    # Convert DataFrame to CSV in memory
    csv_data = df.to_csv(index=False)
    
    # Create a blob (object) in GCS
    blob = bucket.blob(gcs_path)
    
    # Upload the CSV data to GCS
    blob.upload_from_string(csv_data, content_type='text/csv')
    print(f"Data uploaded to gs://{bucket_name}/{gcs_path}")

# Upload datasets to GCS
upload_to_gcs(retail_df, retail_path)
upload_to_gcs(customer_df, customer_path)