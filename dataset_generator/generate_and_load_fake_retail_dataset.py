{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "222e2c09-d257-4931-b6da-5e2ce560413b",
   "metadata": {},
   "outputs": [],
   "source": [
    "import random\n",
    "import pandas as pd\n",
    "from faker import Faker\n",
    "from google.cloud import storage\n",
    "from datetime import datetime\n",
    "\n",
    "# Initialize Faker for generating fake data\n",
    "fake = Faker()\n",
    "\n",
    "# List of possible product categories and products\n",
    "categories = ['Electronics', 'Clothing', 'Home Appliances', 'Books', 'Sports']\n",
    "products = {\n",
    "    'Electronics': ['Phone', 'Laptop', 'Headphones', 'Smartwatch'],\n",
    "    'Clothing': ['T-shirt', 'Jeans', 'Sweater', 'Jacket'],\n",
    "    'Home Appliances': ['Refrigerator', 'Washing Machine', 'Microwave', 'Vacuum Cleaner'],\n",
    "    'Books': ['Novel', 'Biography', 'Science Fiction', 'Fantasy'],\n",
    "    'Sports': ['Football', 'Basketball', 'Tennis Racket', 'Yoga Mat']\n",
    "}\n",
    "\n",
    "# Payment methods\n",
    "payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Cash', 'Gift Card']\n",
    "\n",
    "# Generate Retail Dataset\n",
    "retail_data = []\n",
    "for transaction_id in range(1, 101):\n",
    "    customer_id = random.randint(1, 50)\n",
    "    category = random.choice(categories)\n",
    "    product = random.choice(products[category])\n",
    "    quantity = random.randint(1, 5)\n",
    "    price = random.uniform(5.99, 499.99)\n",
    "    total = round(price * quantity, 2)\n",
    "    date = fake.date_this_year()\n",
    "    payment_method = random.choice(payment_methods)\n",
    "    retail_data.append([transaction_id, customer_id, product, category, quantity, round(price, 2), total, date, payment_method])\n",
    "\n",
    "# Create Retail DataFrame\n",
    "retail_df = pd.DataFrame(retail_data, columns=[\"Transaction_ID\", \"Customer_ID\", \"Product\", \"Category\", \"Quantity\", \"Price\", \"Total\", \"Date\", \"Payment_Method\"])\n",
    "\n",
    "# Generate Customer Dataset\n",
    "customer_data = []\n",
    "for customer_id in range(1, 51):\n",
    "    first_name = fake.first_name()\n",
    "    last_name = fake.last_name()\n",
    "    email = fake.email()\n",
    "    phone = fake.phone_number()\n",
    "    address = fake.address().replace(\"\\n\", \", \")\n",
    "    state = fake.state()\n",
    "    country = \"USA\"\n",
    "    customer_data.append([customer_id, first_name, last_name, email, phone, address, state, country])\n",
    "\n",
    "# Create Customer DataFrame\n",
    "customer_df = pd.DataFrame(customer_data, columns=[\"Customer_ID\", \"First_Name\", \"Last_Name\", \"Email\", \"Phone\", \"Address\", \"State\", \"Country\"])\n",
    "\n",
    "# Set up authentication using the service account JSON key file\n",
    "os.environ[\"GOOGLE_APPLICATION_CREDENTIALS\"] = r\"path_to_your_secrets_key_file\\synthetic-nova-438808-k6-5b8705f92087.json\"\n",
    "\n",
    "# Get today's date in the format YYYY-MM-DD\n",
    "today_date = datetime.today().strftime(\"%Y-%m-%d\")\n",
    "\n",
    "# GCS bucket path\n",
    "# Define your GCS bucket and the base path\n",
    "bucket_name = \"your_bucket_name\"\n",
    "# Create the folder path for today\n",
    "retail_path = f\"retail_data/retail/{today_date}/retail.csv\"\n",
    "customer_path = f\"retail_data/customer/{today_date}/customer.csv\"\n",
    "\n",
    "# Initialize the GCS client\n",
    "client = storage.Client()\n",
    "\n",
    "# Create the GCS bucket object\n",
    "bucket = client.get_bucket(bucket_name)\n",
    "\n",
    "# Function to upload DataFrame to GCS as CSV\n",
    "def upload_to_gcs(df, gcs_path):\n",
    "    # Convert DataFrame to CSV in memory\n",
    "    csv_data = df.to_csv(index=False)\n",
    "    \n",
    "    # Create a blob (object) in GCS\n",
    "    blob = bucket.blob(gcs_path)\n",
    "    \n",
    "    # Upload the CSV data to GCS\n",
    "    blob.upload_from_string(csv_data, content_type='text/csv')\n",
    "    print(f\"Data uploaded to gs://{bucket_name}/{gcs_path}\")\n",
    "\n",
    "\n",
    "upload_to_gcs(retail_df, retail_path)\n",
    "\n",
    "upload_to_gcs(customer_df, customer_path)"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.9.13"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}