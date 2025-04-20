import os
import stripe
import psycopg2
import psycopg2.extras
import datetime
import time
from dotenv import load_dotenv
import logging
from typing import Optional, Any, Dict
import backoff

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv("secrets.env")

# After load_dotenv()
logger.info("Database connection settings:")
logger.info(f"Host: {os.getenv('PG_HOST', 'Not found')}")
logger.info(f"Port: {os.getenv('PG_PORT', 'Not found')}")
logger.info(f"User: {os.getenv('PG_USER', 'Not found')}")
logger.info(f"Password: {'[SET]' if os.getenv('PG_PASSWORD') else '[NOT SET]'}")
logger.info(f"Database: stripe")

# Stripe API configuration
stripe.api_key = os.getenv('STRIPE_API_KEY')

# PostgreSQL configuration
pg_config = {
    'host': os.getenv('PG_HOST', 'localhost'),
    'port': os.getenv('PG_PORT', '5432'),
    'dbname': 'stripe',
    'user': os.getenv('PG_USER', 'postgres'),
    'password': os.getenv('PG_PASSWORD', '')
}

# Utility function for pagination
@backoff.on_exception(backoff.expo, (stripe.error.RateLimitError, stripe.error.APIConnectionError), max_tries=5)
def fetch_all_pages(resource_type: str, params: Dict[str, Any]) -> list:
    """
    Fetch all pages of a Stripe resource with proper error handling and rate limiting.
    """
    all_items = []
    has_more = True
    starting_after = None
    
    while has_more:
        try:
            if starting_after:
                params['starting_after'] = starting_after
                
            # Get the appropriate list method based on resource type
            list_method = getattr(stripe, resource_type.capitalize()).list
            items = list_method(**params)
            
            # Add items to our list
            all_items.extend(items.data)
            
            # Update pagination parameters
            has_more = items.has_more
            if has_more and items.data:
                starting_after = items.data[-1].id
            else:
                has_more = False
                
            # Add a small delay to avoid rate limiting
            time.sleep(0.1)
                
        except stripe.error.StripeError as e:
            logger.error(f"Error fetching {resource_type}: {str(e)}")
            raise
            
    return all_items

# Connect to PostgreSQL with retry logic
@backoff.on_exception(backoff.expo, psycopg2.OperationalError, max_tries=5)
def get_db_connection():
    """
    Get a database connection with retry logic.
    """
    config = {k: v for k, v in pg_config.items() if v}
    return psycopg2.connect(**config)

# Create tables if they don't exist
def setup_database():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Define table schemas
        tables = {
            'payment_intents': '''
                CREATE TABLE IF NOT EXISTS payment_intents (
                    id TEXT PRIMARY KEY,
                    amount INTEGER,
                    object TEXT,
                    review TEXT,
                    source TEXT,
                    status TEXT,
                    created TIMESTAMP,
                    invoice TEXT,
                    currency TEXT,
                    customer TEXT,
                    livemode BOOLEAN,
                    metadata JSONB,
                    shipping JSONB,
                    processing JSONB,
                    application TEXT,
                    canceled_at TIMESTAMP,
                    description TEXT,
                    next_action JSONB,
                    on_behalf_of TEXT,
                    client_secret TEXT,
                    latest_charge TEXT,
                    receipt_email TEXT,
                    transfer_data JSONB,
                    amount_details JSONB,
                    capture_method TEXT,
                    payment_method TEXT,
                    transfer_group TEXT,
                    amount_received INTEGER,
                    amount_capturable INTEGER,
                    last_payment_error JSONB,
                    setup_future_usage TEXT,
                    cancellation_reason TEXT,
                    confirmation_method TEXT,
                    payment_method_types TEXT[],
                    statement_descriptor TEXT,
                    application_fee_amount INTEGER,
                    payment_method_options JSONB,
                    automatic_payment_methods JSONB,
                    statement_descriptor_suffix TEXT,
                    payment_method_configuration_details JSONB,
                    data JSONB
                )
            ''',
            'customers': '''
                CREATE TABLE IF NOT EXISTS customers (
                    id TEXT PRIMARY KEY,
                    email TEXT,
                    name TEXT,
                    description TEXT,
                    created TIMESTAMP,
                    metadata JSONB,
                    data JSONB
                )
            ''',
            'products': '''
                CREATE TABLE IF NOT EXISTS products (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    description TEXT,
                    active BOOLEAN,
                    created TIMESTAMP,
                    metadata JSONB,
                    data JSONB
                )
            ''',
            'prices': '''
                CREATE TABLE IF NOT EXISTS prices (
                    id TEXT PRIMARY KEY,
                    product_id TEXT REFERENCES products(id),
                    unit_amount INTEGER,
                    currency TEXT,
                    active BOOLEAN,
                    type TEXT,
                    created TIMESTAMP,
                    metadata JSONB,
                    data JSONB
                )
            ''',
            'subscriptions': '''
                CREATE TABLE IF NOT EXISTS subscriptions (
                    id TEXT PRIMARY KEY,
                    customer_id TEXT REFERENCES customers(id),
                    status TEXT,
                    current_period_start TIMESTAMP,
                    current_period_end TIMESTAMP,
                    created TIMESTAMP,
                    metadata JSONB,
                    data JSONB
                )
            ''',
            'invoices': '''
                CREATE TABLE IF NOT EXISTS invoices (
                    id TEXT PRIMARY KEY,
                    customer_id TEXT REFERENCES customers(id),
                    subscription_id TEXT REFERENCES subscriptions(id),
                    status TEXT,
                    amount_due INTEGER,
                    amount_paid INTEGER,
                    currency TEXT,
                    created TIMESTAMP,
                    metadata JSONB,
                    data JSONB
                )
            ''',
            'charges': '''
                CREATE TABLE IF NOT EXISTS charges (
                    id TEXT PRIMARY KEY,
                    customer_id TEXT REFERENCES customers(id),
                    payment_intent_id TEXT REFERENCES payment_intents(id),
                    amount INTEGER,
                    currency TEXT,
                    status TEXT,
                    created TIMESTAMP,
                    metadata JSONB,
                    data JSONB
                )
            ''',
            'payment_methods': '''
                CREATE TABLE IF NOT EXISTS payment_methods (
                    id TEXT PRIMARY KEY,
                    customer_id TEXT REFERENCES customers(id),
                    type TEXT,
                    created TIMESTAMP,
                    metadata JSONB,
                    data JSONB
                )
            ''',
            'sync_log': '''
                CREATE TABLE IF NOT EXISTS sync_log (
                    resource_name TEXT PRIMARY KEY,
                    last_sync_timestamp TIMESTAMP
                )
            '''
        }
        
        for table_name, create_query in tables.items():
            cursor.execute(create_query)
        
        conn.commit()
        print("Database tables created successfully")
            
    except Exception as e:
        print(f"Error setting up database: {str(e)}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

# Get the timestamp of the last sync for a resource
def get_last_sync(resource_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute(
        "SELECT last_sync_timestamp FROM sync_log WHERE resource_name = %s",
        (resource_name,)
    )
    result = cursor.fetchone()
    
    cursor.close()
    conn.close()
    
    if result:
        return result[0]
    return None

# Update the timestamp of the last sync for a resource
def update_last_sync(resource_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    now = datetime.datetime.now()
    
    cursor.execute(
        """
        INSERT INTO sync_log (resource_name, last_sync_timestamp)
        VALUES (%s, %s)
        ON CONFLICT (resource_name) DO UPDATE
        SET last_sync_timestamp = EXCLUDED.last_sync_timestamp
        """,
        (resource_name, now)
    )
    
    conn.commit()
    cursor.close()
    conn.close()

# Sync customers from Stripe to PostgreSQL
def sync_customers():
    print("Syncing customers...")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Get current count before sync
        cursor.execute("SELECT COUNT(*) FROM customers")
        initial_count = cursor.fetchone()[0]
        print(f"Current records in database: {initial_count}")
        
        # Get the latest created timestamp from the database
        cursor.execute("SELECT MAX(created) FROM customers")
        last_created = cursor.fetchone()[0]
        
        params = {'limit': 100}
        if last_created:
            # Convert datetime to Unix timestamp
            last_created_timestamp = int(last_created.timestamp())
            print(f"Fetching customers created after: {last_created} (timestamp: {last_created_timestamp})")
            params['created'] = {'gt': last_created_timestamp}
        else:
            print("No existing records found, fetching all customers")
        
        has_more = True
        starting_after = None
        total_count = 0
        page_count = 0
        new_records = 0
        updated_records = 0
        failed_records = 0
        
        while has_more:
            try:
                if starting_after:
                    params['starting_after'] = starting_after
                    
                customers = stripe.Customer.list(**params)
                page_count += 1
                count = 0
                
                if not customers.data:
                    print(f"No customers found in page {page_count}")
                    break
                
                for customer in customers.auto_paging_iter():
                    try:
                        # Start a new transaction for each customer
                        conn.rollback()  # Ensure we start fresh
                        cursor = conn.cursor()
                        
                        created_date = datetime.datetime.fromtimestamp(customer.created)
                        
                        # Check if record exists
                        cursor.execute("SELECT id FROM customers WHERE id = %s", (customer.id,))
                        exists = cursor.fetchone() is not None
                        
                        # Prepare the data for insertion
                        data = {
                            'id': customer.id,
                            'email': customer.email,
                            'name': customer.name,
                            'description': customer.description,
                            'created': created_date,
                            'metadata': customer.metadata,
                            'data': customer
                        }
                        
                        # Convert JSON fields to proper format
                        for field in ['metadata', 'data']:
                            if data[field] is not None:
                                data[field] = psycopg2.extras.Json(data[field])
                        
                        # Build the SQL query dynamically
                        columns = ', '.join(data.keys())
                        values = ', '.join(['%s'] * len(data))
                        update_clause = ', '.join([f"{k} = EXCLUDED.{k}" for k in data.keys()])
                        
                        query = f"""
                            INSERT INTO customers ({columns})
                            VALUES ({values})
                            ON CONFLICT (id) DO UPDATE SET {update_clause}
                        """
                        
                        cursor.execute(query, list(data.values()))
                        conn.commit()  # Commit after each customer
                        
                        if exists:
                            updated_records += 1
                        else:
                            new_records += 1
                            
                        count += 1
                        total_count += 1
                        
                        # Print progress for each customer
                        print(f"Processed customer {customer.id} (Total: {total_count}, New: {new_records}, Updated: {updated_records}, Failed: {failed_records})")
                        
                    except Exception as e:
                        print(f"Error processing customer {customer.id}: {str(e)}")
                        print(f"Data content: {data}")
                        failed_records += 1
                        conn.rollback()
                        continue
                
                print(f"Completed page {page_count}: Processed {count} customers")
                
                has_more = customers.has_more
                if has_more and len(customers.data) > 0:
                    starting_after = customers.data[-1].id
                else:
                    has_more = False
                
            except Exception as e:
                print(f"Error processing page {page_count}: {str(e)}")
                conn.rollback()
                continue
        
        # Get final count
        cursor.execute("SELECT COUNT(*) FROM customers")
        final_count = cursor.fetchone()[0]
        
        print("\nCustomers Sync Summary:")
        print(f"Total pages processed: {page_count}")
        print(f"Total records processed: {total_count}")
        print(f"New records added: {new_records}")
        print(f"Records updated: {updated_records}")
        print(f"Records failed: {failed_records}")
        print(f"Initial database count: {initial_count}")
        print(f"Final database count: {final_count}")
        print(f"Net change in database: {final_count - initial_count}")
        
    except Exception as e:
        print(f"Error in sync_customers: {str(e)}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

# Sync products from Stripe to PostgreSQL
def sync_products():
    print("Syncing products...")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Get current count before sync
        cursor.execute("SELECT COUNT(*) FROM products")
        initial_count = cursor.fetchone()[0]
        print(f"Current records in database: {initial_count}")
        
        # Get the latest created timestamp from the database
        cursor.execute("SELECT MAX(created) FROM products")
        last_created = cursor.fetchone()[0]
        
        params = {'limit': 100}
        if last_created:
            # Convert datetime to Unix timestamp
            last_created_timestamp = int(last_created.timestamp())
            print(f"Fetching products created after: {last_created} (timestamp: {last_created_timestamp})")
            params['created'] = {'gt': last_created_timestamp}
        else:
            print("No existing records found, fetching all products")
        
        has_more = True
        starting_after = None
        total_count = 0
        page_count = 0
        new_records = 0
        updated_records = 0
        failed_records = 0
        
        while has_more:
            try:
                if starting_after:
                    params['starting_after'] = starting_after
                    
                products = stripe.Product.list(**params)
                page_count += 1
                count = 0
                
                if not products.data:
                    print(f"No products found in page {page_count}")
                    break
                
                for product in products.auto_paging_iter():
                    try:
                        # Start a new transaction for each product
                        conn.rollback()  # Ensure we start fresh
                        cursor = conn.cursor()
                        
                        created_date = datetime.datetime.fromtimestamp(product.created)
                        
                        # Check if record exists
                        cursor.execute("SELECT id FROM products WHERE id = %s", (product.id,))
                        exists = cursor.fetchone() is not None
                        
                        # Prepare the data for insertion
                        data = {
                            'id': product.id,
                            'name': product.name,
                            'description': product.description,
                            'active': product.active,
                            'created': created_date,
                            'metadata': product.metadata,
                            'data': product
                        }
                        
                        # Convert JSON fields to proper format
                        for field in ['metadata', 'data']:
                            if data[field] is not None:
                                data[field] = psycopg2.extras.Json(data[field])
                        
                        # Build the SQL query dynamically
                        columns = ', '.join(data.keys())
                        values = ', '.join(['%s'] * len(data))
                        update_clause = ', '.join([f"{k} = EXCLUDED.{k}" for k in data.keys()])
                        
                        query = f"""
                            INSERT INTO products ({columns})
                            VALUES ({values})
                            ON CONFLICT (id) DO UPDATE SET {update_clause}
                        """
                        
                        cursor.execute(query, list(data.values()))
                        conn.commit()  # Commit after each product
                        
                        if exists:
                            updated_records += 1
                        else:
                            new_records += 1
                            
                        count += 1
                        total_count += 1
                        
                        # Print progress for each product
                        print(f"Processed product {product.id} (Total: {total_count}, New: {new_records}, Updated: {updated_records}, Failed: {failed_records})")
                        
                    except Exception as e:
                        print(f"Error processing product {product.id}: {str(e)}")
                        print(f"Data content: {data}")
                        failed_records += 1
                        conn.rollback()
                        continue
                
                print(f"Completed page {page_count}: Processed {count} products")
                
                has_more = products.has_more
                if has_more and len(products.data) > 0:
                    starting_after = products.data[-1].id
                else:
                    has_more = False
                
            except Exception as e:
                print(f"Error processing page {page_count}: {str(e)}")
                conn.rollback()
                continue
        
        # Get final count
        cursor.execute("SELECT COUNT(*) FROM products")
        final_count = cursor.fetchone()[0]
        
        print("\nProducts Sync Summary:")
        print(f"Total pages processed: {page_count}")
        print(f"Total records processed: {total_count}")
        print(f"New records added: {new_records}")
        print(f"Records updated: {updated_records}")
        print(f"Records failed: {failed_records}")
        print(f"Initial database count: {initial_count}")
        print(f"Final database count: {final_count}")
        print(f"Net change in database: {final_count - initial_count}")
        
    except Exception as e:
        print(f"Error in sync_products: {str(e)}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

# Sync prices from Stripe to PostgreSQL
def sync_prices():
    print("Syncing prices...")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Get current count before sync
        cursor.execute("SELECT COUNT(*) FROM prices")
        initial_count = cursor.fetchone()[0]
        print(f"Current records in database: {initial_count}")
        
        # Get the latest created timestamp from the database
        cursor.execute("SELECT MAX(created) FROM prices")
        last_created = cursor.fetchone()[0]
        
        params = {'limit': 100}
        if last_created:
            # Convert datetime to Unix timestamp
            last_created_timestamp = int(last_created.timestamp())
            print(f"Fetching prices created after: {last_created} (timestamp: {last_created_timestamp})")
            params['created'] = {'gt': last_created_timestamp}
        else:
            print("No existing records found, fetching all prices")
        
        has_more = True
        starting_after = None
        total_count = 0
        page_count = 0
        new_records = 0
        updated_records = 0
        failed_records = 0
        
        while has_more:
            try:
                if starting_after:
                    params['starting_after'] = starting_after
                    
                prices = stripe.Price.list(**params)
                page_count += 1
                count = 0
                
                if not prices.data:
                    print(f"No prices found in page {page_count}")
                    break
                
                for price in prices.auto_paging_iter():
                    try:
                        # Start a new transaction for each price
                        conn.rollback()  # Ensure we start fresh
                        cursor = conn.cursor()
                        
                        created_date = datetime.datetime.fromtimestamp(price.created)
                        
                        # Check if record exists
                        cursor.execute("SELECT id FROM prices WHERE id = %s", (price.id,))
                        exists = cursor.fetchone() is not None
                        
                        # Prepare the data for insertion
                        data = {
                            'id': price.id,
                            'product_id': price.product,
                            'unit_amount': price.unit_amount,
                            'currency': price.currency,
                            'active': price.active,
                            'type': price.type,
                            'created': created_date,
                            'metadata': price.metadata,
                            'data': price
                        }
                        
                        # Convert JSON fields to proper format
                        for field in ['metadata', 'data']:
                            if data[field] is not None:
                                data[field] = psycopg2.extras.Json(data[field])
                        
                        # Build the SQL query dynamically
                        columns = ', '.join(data.keys())
                        values = ', '.join(['%s'] * len(data))
                        update_clause = ', '.join([f"{k} = EXCLUDED.{k}" for k in data.keys()])
                        
                        query = f"""
                            INSERT INTO prices ({columns})
                            VALUES ({values})
                            ON CONFLICT (id) DO UPDATE SET {update_clause}
                        """
                        
                        cursor.execute(query, list(data.values()))
                        conn.commit()  # Commit after each price
                        
                        if exists:
                            updated_records += 1
                        else:
                            new_records += 1
                            
                        count += 1
                        total_count += 1
                        
                        # Print progress for each price
                        print(f"Processed price {price.id} (Total: {total_count}, New: {new_records}, Updated: {updated_records}, Failed: {failed_records})")
                        
                    except Exception as e:
                        print(f"Error processing price {price.id}: {str(e)}")
                        print(f"Data content: {data}")
                        failed_records += 1
                        conn.rollback()
                        continue
                
                print(f"Completed page {page_count}: Processed {count} prices")
                
                has_more = prices.has_more
                if has_more and len(prices.data) > 0:
                    starting_after = prices.data[-1].id
                else:
                    has_more = False
                
            except Exception as e:
                print(f"Error processing page {page_count}: {str(e)}")
                conn.rollback()
                continue
        
        # Get final count
        cursor.execute("SELECT COUNT(*) FROM prices")
        final_count = cursor.fetchone()[0]
        
        print("\nPrices Sync Summary:")
        print(f"Total pages processed: {page_count}")
        print(f"Total records processed: {total_count}")
        print(f"New records added: {new_records}")
        print(f"Records updated: {updated_records}")
        print(f"Records failed: {failed_records}")
        print(f"Initial database count: {initial_count}")
        print(f"Final database count: {final_count}")
        print(f"Net change in database: {final_count - initial_count}")
        
    except Exception as e:
        print(f"Error in sync_prices: {str(e)}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

# Sync subscriptions from Stripe to PostgreSQL
def sync_subscriptions():
    print("Syncing subscriptions...")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Get current count before sync
        cursor.execute("SELECT COUNT(*) FROM subscriptions")
        initial_count = cursor.fetchone()[0]
        print(f"Current records in database: {initial_count}")
        
        # Get the latest created timestamp from the database
        cursor.execute("SELECT MAX(created) FROM subscriptions")
        last_created = cursor.fetchone()[0]
        
        params = {'limit': 100}
        if last_created:
            # Convert datetime to Unix timestamp
            last_created_timestamp = int(last_created.timestamp())
            print(f"Fetching subscriptions created after: {last_created} (timestamp: {last_created_timestamp})")
            params['created'] = {'gt': last_created_timestamp}
        else:
            print("No existing records found, fetching all subscriptions")
        
        has_more = True
        starting_after = None
        total_count = 0
        page_count = 0
        new_records = 0
        updated_records = 0
        failed_records = 0
        
        while has_more:
            try:
                if starting_after:
                    params['starting_after'] = starting_after
                    
                subscriptions = stripe.Subscription.list(**params)
                page_count += 1
                count = 0
                
                if not subscriptions.data:
                    print(f"No subscriptions found in page {page_count}")
                    break
                
                for subscription in subscriptions.auto_paging_iter():
                    try:
                        # Start a new transaction for each subscription
                        conn.rollback()  # Ensure we start fresh
                        cursor = conn.cursor()
                        
                        created_date = datetime.datetime.fromtimestamp(subscription.created)
                        current_period_start = datetime.datetime.fromtimestamp(subscription.current_period_start)
                        current_period_end = datetime.datetime.fromtimestamp(subscription.current_period_end)
                        
                        # Check if record exists
                        cursor.execute("SELECT id FROM subscriptions WHERE id = %s", (subscription.id,))
                        exists = cursor.fetchone() is not None
                        
                        # Prepare the data for insertion
                        data = {
                            'id': subscription.id,
                            'customer_id': subscription.customer,
                            'status': subscription.status,
                            'current_period_start': current_period_start,
                            'current_period_end': current_period_end,
                            'created': created_date,
                            'metadata': subscription.metadata,
                            'data': subscription
                        }
                        
                        # Convert JSON fields to proper format
                        for field in ['metadata', 'data']:
                            if data[field] is not None:
                                data[field] = psycopg2.extras.Json(data[field])
                        
                        # Build the SQL query dynamically
                        columns = ', '.join(data.keys())
                        values = ', '.join(['%s'] * len(data))
                        update_clause = ', '.join([f"{k} = EXCLUDED.{k}" for k in data.keys()])
                        
                        query = f"""
                            INSERT INTO subscriptions ({columns})
                            VALUES ({values})
                            ON CONFLICT (id) DO UPDATE SET {update_clause}
                        """
                        
                        cursor.execute(query, list(data.values()))
                        conn.commit()  # Commit after each subscription
                        
                        if exists:
                            updated_records += 1
                        else:
                            new_records += 1
                            
                        count += 1
                        total_count += 1
                        
                        # Print progress for each subscription
                        print(f"Processed subscription {subscription.id} (Total: {total_count}, New: {new_records}, Updated: {updated_records}, Failed: {failed_records})")
                        
                    except Exception as e:
                        print(f"Error processing subscription {subscription.id}: {str(e)}")
                        print(f"Data content: {data}")
                        failed_records += 1
                        conn.rollback()
                        continue
                
                print(f"Completed page {page_count}: Processed {count} subscriptions")
                
                has_more = subscriptions.has_more
                if has_more and len(subscriptions.data) > 0:
                    starting_after = subscriptions.data[-1].id
                else:
                    has_more = False
                
            except Exception as e:
                print(f"Error processing page {page_count}: {str(e)}")
                conn.rollback()
                continue
        
        # Get final count
        cursor.execute("SELECT COUNT(*) FROM subscriptions")
        final_count = cursor.fetchone()[0]
        
        print("\nSubscriptions Sync Summary:")
        print(f"Total pages processed: {page_count}")
        print(f"Total records processed: {total_count}")
        print(f"New records added: {new_records}")
        print(f"Records updated: {updated_records}")
        print(f"Records failed: {failed_records}")
        print(f"Initial database count: {initial_count}")
        print(f"Final database count: {final_count}")
        print(f"Net change in database: {final_count - initial_count}")
        
    except Exception as e:
        print(f"Error in sync_subscriptions: {str(e)}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

# Sync invoices from Stripe to PostgreSQL
def sync_invoices():
    print("Syncing invoices...")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Get current count before sync
        cursor.execute("SELECT COUNT(*) FROM invoices")
        initial_count = cursor.fetchone()[0]
        print(f"Current records in database: {initial_count}")
        
        # Get the latest created timestamp from the database
        cursor.execute("SELECT MAX(created) FROM invoices")
        last_created = cursor.fetchone()[0]
        
        params = {'limit': 100}
        if last_created:
            # Convert datetime to Unix timestamp
            last_created_timestamp = int(last_created.timestamp())
            print(f"Fetching invoices created after: {last_created} (timestamp: {last_created_timestamp})")
            params['created'] = {'gt': last_created_timestamp}
        else:
            print("No existing records found, fetching all invoices")
        
        has_more = True
        starting_after = None
        total_count = 0
        page_count = 0
        new_records = 0
        updated_records = 0
        failed_records = 0
        
        while has_more:
            try:
                if starting_after:
                    params['starting_after'] = starting_after
                    
                invoices = stripe.Invoice.list(**params)
                page_count += 1
                count = 0
                
                if not invoices.data:
                    print(f"No invoices found in page {page_count}")
                    break
                
                for invoice in invoices.auto_paging_iter():
                    try:
                        # Start a new transaction for each invoice
                        conn.rollback()  # Ensure we start fresh
                        cursor = conn.cursor()
                        
                        created_date = datetime.datetime.fromtimestamp(invoice.created)
                        
                        # Check if record exists
                        cursor.execute("SELECT id FROM invoices WHERE id = %s", (invoice.id,))
                        exists = cursor.fetchone() is not None
                        
                        # Prepare the data for insertion
                        data = {
                            'id': invoice.id,
                            'customer_id': invoice.customer,
                            'subscription_id': invoice.subscription,
                            'status': invoice.status,
                            'amount_due': invoice.amount_due,
                            'amount_paid': invoice.amount_paid,
                            'currency': invoice.currency,
                            'created': created_date,
                            'metadata': invoice.metadata,
                            'data': invoice
                        }
                        
                        # Convert JSON fields to proper format
                        for field in ['metadata', 'data']:
                            if data[field] is not None:
                                data[field] = psycopg2.extras.Json(data[field])
                        
                        # Build the SQL query dynamically
                        columns = ', '.join(data.keys())
                        values = ', '.join(['%s'] * len(data))
                        update_clause = ', '.join([f"{k} = EXCLUDED.{k}" for k in data.keys()])
                        
                        query = f"""
                            INSERT INTO invoices ({columns})
                            VALUES ({values})
                            ON CONFLICT (id) DO UPDATE SET {update_clause}
                        """
                        
                        cursor.execute(query, list(data.values()))
                        conn.commit()  # Commit after each invoice
                        
                        if exists:
                            updated_records += 1
                        else:
                            new_records += 1
                            
                        count += 1
                        total_count += 1
                        
                        # Print progress for each invoice
                        print(f"Processed invoice {invoice.id} (Total: {total_count}, New: {new_records}, Updated: {updated_records}, Failed: {failed_records})")
                        
                    except Exception as e:
                        print(f"Error processing invoice {invoice.id}: {str(e)}")
                        print(f"Data content: {data}")
                        failed_records += 1
                        conn.rollback()
                        continue
                
                print(f"Completed page {page_count}: Processed {count} invoices")
                
                has_more = invoices.has_more
                if has_more and len(invoices.data) > 0:
                    starting_after = invoices.data[-1].id
                else:
                    has_more = False
                
            except Exception as e:
                print(f"Error processing page {page_count}: {str(e)}")
                conn.rollback()
                continue
        
        # Get final count
        cursor.execute("SELECT COUNT(*) FROM invoices")
        final_count = cursor.fetchone()[0]
        
        print("\nInvoices Sync Summary:")
        print(f"Total pages processed: {page_count}")
        print(f"Total records processed: {total_count}")
        print(f"New records added: {new_records}")
        print(f"Records updated: {updated_records}")
        print(f"Records failed: {failed_records}")
        print(f"Initial database count: {initial_count}")
        print(f"Final database count: {final_count}")
        print(f"Net change in database: {final_count - initial_count}")
        
    except Exception as e:
        print(f"Error in sync_invoices: {str(e)}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

# Sync payment intents from Stripe to PostgreSQL
def sync_payment_intents():
    print("Syncing payment intents...")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Get current count before sync
        cursor.execute("SELECT COUNT(*) FROM payment_intents")
        initial_count = cursor.fetchone()[0]
        print(f"Current records in database: {initial_count}")
        
        # Get the latest created timestamp from the database
        cursor.execute("SELECT MAX(created) FROM payment_intents")
        last_created = cursor.fetchone()[0]
        
        params = {'limit': 100}
        if last_created:
            # Convert datetime to Unix timestamp
            last_created_timestamp = int(last_created.timestamp())
            print(f"Fetching payment intents created after: {last_created} (timestamp: {last_created_timestamp})")
            params['created'] = {'gt': last_created_timestamp}
        else:
            print("No existing records found, fetching all payment intents")
        
        has_more = True
        starting_after = None
        total_count = 0
        page_count = 0
        new_records = 0
        updated_records = 0
        failed_records = 0
        
        while has_more:
            try:
                if starting_after:
                    params['starting_after'] = starting_after
                    
                payment_intents = stripe.PaymentIntent.list(**params)
                page_count += 1
                count = 0
                
                if not payment_intents.data:
                    print(f"No payment intents found in page {page_count}")
                    break
                
                for payment_intent in payment_intents.auto_paging_iter():
                    try:
                        # Start a new transaction for each payment intent
                        conn.rollback()  # Ensure we start fresh
                        cursor = conn.cursor()
                        
                        created_date = datetime.datetime.fromtimestamp(payment_intent.created)
                        canceled_at = datetime.datetime.fromtimestamp(payment_intent.canceled_at) if payment_intent.canceled_at else None
                        
                        # Check if record exists
                        cursor.execute("SELECT id FROM payment_intents WHERE id = %s", (payment_intent.id,))
                        exists = cursor.fetchone() is not None
                        
                        # Prepare the data for insertion
                        data = {
                            'id': payment_intent.id,
                            'amount': payment_intent.amount,
                            'object': payment_intent.object,
                            'review': payment_intent.review if hasattr(payment_intent, 'review') else None,
                            'source': payment_intent.source if hasattr(payment_intent, 'source') else None,
                            'status': payment_intent.status,
                            'created': created_date,
                            'invoice': payment_intent.invoice if hasattr(payment_intent, 'invoice') else None,
                            'currency': payment_intent.currency,
                            'customer': payment_intent.customer,
                            'livemode': payment_intent.livemode,
                            'metadata': payment_intent.metadata,
                            'shipping': payment_intent.shipping if hasattr(payment_intent, 'shipping') else None,
                            'processing': payment_intent.processing if hasattr(payment_intent, 'processing') else None,
                            'application': payment_intent.application if hasattr(payment_intent, 'application') else None,
                            'canceled_at': canceled_at,
                            'description': payment_intent.description if hasattr(payment_intent, 'description') else None,
                            'next_action': payment_intent.next_action if hasattr(payment_intent, 'next_action') else None,
                            'on_behalf_of': payment_intent.on_behalf_of if hasattr(payment_intent, 'on_behalf_of') else None,
                            'client_secret': payment_intent.client_secret,
                            'latest_charge': payment_intent.latest_charge if hasattr(payment_intent, 'latest_charge') else None,
                            'receipt_email': payment_intent.receipt_email if hasattr(payment_intent, 'receipt_email') else None,
                            'transfer_data': payment_intent.transfer_data if hasattr(payment_intent, 'transfer_data') else None,
                            'amount_details': payment_intent.amount_details if hasattr(payment_intent, 'amount_details') else None,
                            'capture_method': payment_intent.capture_method if hasattr(payment_intent, 'capture_method') else None,
                            'payment_method': payment_intent.payment_method if hasattr(payment_intent, 'payment_method') else None,
                            'transfer_group': payment_intent.transfer_group if hasattr(payment_intent, 'transfer_group') else None,
                            'amount_received': payment_intent.amount_received if hasattr(payment_intent, 'amount_received') else None,
                            'amount_capturable': payment_intent.amount_capturable if hasattr(payment_intent, 'amount_capturable') else None,
                            'last_payment_error': payment_intent.last_payment_error if hasattr(payment_intent, 'last_payment_error') else None,
                            'setup_future_usage': payment_intent.setup_future_usage if hasattr(payment_intent, 'setup_future_usage') else None,
                            'cancellation_reason': payment_intent.cancellation_reason if hasattr(payment_intent, 'cancellation_reason') else None,
                            'confirmation_method': payment_intent.confirmation_method if hasattr(payment_intent, 'confirmation_method') else None,
                            'payment_method_types': payment_intent.payment_method_types if hasattr(payment_intent, 'payment_method_types') else None,
                            'statement_descriptor': payment_intent.statement_descriptor if hasattr(payment_intent, 'statement_descriptor') else None,
                            'application_fee_amount': payment_intent.application_fee_amount if hasattr(payment_intent, 'application_fee_amount') else None,
                            'payment_method_options': payment_intent.payment_method_options if hasattr(payment_intent, 'payment_method_options') else None,
                            'automatic_payment_methods': payment_intent.automatic_payment_methods if hasattr(payment_intent, 'automatic_payment_methods') else None,
                            'statement_descriptor_suffix': payment_intent.statement_descriptor_suffix if hasattr(payment_intent, 'statement_descriptor_suffix') else None,
                            'payment_method_configuration_details': payment_intent.payment_method_configuration_details if hasattr(payment_intent, 'payment_method_configuration_details') else None,
                            'data': payment_intent  # Store the complete payment intent object
                        }
                        
                        # Convert JSON fields to proper format
                        for field in ['metadata', 'shipping', 'processing', 'next_action', 'transfer_data', 
                                    'amount_details', 'last_payment_error', 'payment_method_options', 
                                    'automatic_payment_methods', 'payment_method_configuration_details', 'data']:
                            if data[field] is not None:
                                data[field] = psycopg2.extras.Json(data[field])
                        
                        # Convert payment_method_types to array if it exists
                        if data['payment_method_types'] is not None:
                            if isinstance(data['payment_method_types'], list):
                                data['payment_method_types'] = data['payment_method_types']
                            else:
                                data['payment_method_types'] = [data['payment_method_types']]
                        
                        # Build the SQL query dynamically
                        columns = ', '.join(data.keys())
                        values = ', '.join(['%s'] * len(data))
                        update_clause = ', '.join([f"{k} = EXCLUDED.{k}" for k in data.keys()])
                        
                        query = f"""
                            INSERT INTO payment_intents ({columns})
                            VALUES ({values})
                            ON CONFLICT (id) DO UPDATE SET {update_clause}
                        """
                        
                        cursor.execute(query, list(data.values()))
                        conn.commit()  # Commit after each payment intent
                        
                        if exists:
                            updated_records += 1
                        else:
                            new_records += 1
                            
                        count += 1
                        total_count += 1
                        
                        # Print progress for each payment intent
                        print(f"Processed payment intent {payment_intent.id} (Total: {total_count}, New: {new_records}, Updated: {updated_records}, Failed: {failed_records})")
                        
                    except Exception as e:
                        print(f"Error processing payment intent {payment_intent.id}: {str(e)}")
                        print(f"Data content: {data}")
                        failed_records += 1
                        conn.rollback()
                        continue
                
                print(f"Completed page {page_count}: Processed {count} payment intents")
                
                has_more = payment_intents.has_more
                if has_more and len(payment_intents.data) > 0:
                    starting_after = payment_intents.data[-1].id
                else:
                    has_more = False
                
            except Exception as e:
                print(f"Error processing page {page_count}: {str(e)}")
                conn.rollback()
                # Don't raise the exception, continue with the next page
                continue
        
        # Get final count
        cursor.execute("SELECT COUNT(*) FROM payment_intents")
        final_count = cursor.fetchone()[0]
        
        print("\nPayment Intents Sync Summary:")
        print(f"Total pages processed: {page_count}")
        print(f"Total records processed: {total_count}")
        print(f"New records added: {new_records}")
        print(f"Records updated: {updated_records}")
        print(f"Records failed: {failed_records}")
        print(f"Initial database count: {initial_count}")
        print(f"Final database count: {final_count}")
        print(f"Net change in database: {final_count - initial_count}")
        
    except Exception as e:
        print(f"Error in sync_payment_intents: {str(e)}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

# Sync charges from Stripe to PostgreSQL
def sync_charges():
    print("Syncing charges...")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Get current count before sync
        cursor.execute("SELECT COUNT(*) FROM charges")
        initial_count = cursor.fetchone()[0]
        print(f"Current records in database: {initial_count}")
        
        # Get the latest created timestamp from the database
        cursor.execute("SELECT MAX(created) FROM charges")
        last_created = cursor.fetchone()[0]
        
        params = {'limit': 100, 'expand': ['data.payment_intent']}  # Expand payment_intent to get full details
        if last_created:
            # Convert datetime to Unix timestamp
            last_created_timestamp = int(last_created.timestamp())
            print(f"Fetching charges created after: {last_created} (timestamp: {last_created_timestamp})")
            params['created'] = {'gt': last_created_timestamp}
        else:
            print("No existing records found, fetching all charges")
        
        has_more = True
        starting_after = None
        total_count = 0
        page_count = 0
        payment_intents_added = 0
        new_records = 0
        updated_records = 0
        failed_records = 0
        
        while has_more:
            try:
                if starting_after:
                    params['starting_after'] = starting_after
                    
                print(f"Fetching page {page_count + 1} with params: {params}")
                charges = stripe.Charge.list(**params)
                page_count += 1
                count = 0
                
                if not charges.data:
                    print(f"No charges found in page {page_count}")
                    break
                
                for charge in charges.auto_paging_iter():
                    try:
                        created_date = datetime.datetime.fromtimestamp(charge.created)
                        customer_id = charge.customer if hasattr(charge, 'customer') else None
                        payment_intent_id = charge.payment_intent if hasattr(charge, 'payment_intent') else None
                        
                        # Check if record exists
                        cursor.execute("SELECT id FROM charges WHERE id = %s", (charge.id,))
                        exists = cursor.fetchone() is not None
                        
                        # If there's a payment_intent_id, ensure it exists in our database
                        if payment_intent_id:
                            cursor.execute(
                                "SELECT id FROM payment_intents WHERE id = %s",
                                (payment_intent_id,)
                            )
                            if not cursor.fetchone():
                                # Fetch the payment intent from Stripe
                                try:
                                    payment_intent = stripe.PaymentIntent.retrieve(payment_intent_id)
                                    created_date = datetime.datetime.fromtimestamp(payment_intent.created)
                                    customer_id = payment_intent.customer if hasattr(payment_intent, 'customer') else None
                                    
                                    # Prepare payment intent data
                                    pi_data = {
                                        'id': payment_intent.id,
                                        'amount': payment_intent.amount,
                                        'status': payment_intent.status,
                                        'created': created_date,
                                        'currency': payment_intent.currency,
                                        'customer': customer_id,
                                        'livemode': payment_intent.livemode,
                                        'metadata': payment_intent.metadata,
                                        'client_secret': payment_intent.client_secret,
                                        'data': payment_intent
                                    }
                                    
                                    # Convert JSON fields
                                    for field in ['metadata', 'data']:
                                        if pi_data[field] is not None:
                                            pi_data[field] = psycopg2.extras.Json(pi_data[field])
                                    
                                    # Build the SQL query
                                    columns = ', '.join(pi_data.keys())
                                    values = ', '.join(['%s'] * len(pi_data))
                                    update_clause = ', '.join([f"{k} = EXCLUDED.{k}" for k in pi_data.keys()])
                                    
                                    query = f"""
                                        INSERT INTO payment_intents ({columns})
                                        VALUES ({values})
                                        ON CONFLICT (id) DO UPDATE SET {update_clause}
                                    """
                                    
                                    cursor.execute(query, list(pi_data.values()))
                                    payment_intents_added += 1
                                    conn.commit()
                                except stripe.error.InvalidRequestError as e:
                                    print(f"Error fetching payment intent {payment_intent_id}: {str(e)}")
                                    continue
                        
                        # Prepare charge data
                        charge_data = {
                            'id': charge.id,
                            'customer_id': customer_id,
                            'payment_intent_id': payment_intent_id,
                            'amount': charge.amount,
                            'currency': charge.currency,
                            'status': charge.status,
                            'created': created_date,
                            'metadata': charge.metadata,
                            'data': charge
                        }
                        
                        # Convert JSON fields
                        for field in ['metadata', 'data']:
                            if charge_data[field] is not None:
                                charge_data[field] = psycopg2.extras.Json(charge_data[field])
                        
                        # Build the SQL query
                        columns = ', '.join(charge_data.keys())
                        values = ', '.join(['%s'] * len(charge_data))
                        update_clause = ', '.join([f"{k} = EXCLUDED.{k}" for k in charge_data.keys()])
                        
                        query = f"""
                            INSERT INTO charges ({columns})
                            VALUES ({values})
                            ON CONFLICT (id) DO UPDATE SET {update_clause}
                        """
                        
                        cursor.execute(query, list(charge_data.values()))
                        conn.commit()  # Commit after each charge
                        
                        if exists:
                            updated_records += 1
                        else:
                            new_records += 1
                            
                        count += 1
                        total_count += 1
                        
                    except Exception as e:
                        print(f"Error processing charge {charge.id}: {str(e)}")
                        failed_records += 1
                        conn.rollback()
                        continue
                
                # Commit after each page
                conn.commit()
                print(f"Page {page_count}: Processed {count} charges (New: {new_records}, Updated: {updated_records}, Failed: {failed_records}, Added PIs: {payment_intents_added})")
                
                has_more = charges.has_more
                if has_more and len(charges.data) > 0:
                    starting_after = charges.data[-1].id
                else:
                    has_more = False
                    
                # Update last sync timestamp after each successful page
                update_last_sync('charges')
                
            except Exception as e:
                print(f"Error processing page {page_count}: {str(e)}")
                conn.rollback()
                raise
        
        # Get final count
        cursor.execute("SELECT COUNT(*) FROM charges")
        final_count = cursor.fetchone()[0]
        
        print("\nCharges Sync Summary:")
        print(f"Total pages processed: {page_count}")
        print(f"Total records processed: {total_count}")
        print(f"New records added: {new_records}")
        print(f"Records updated: {updated_records}")
        print(f"Records failed: {failed_records}")
        print(f"Payment intents added: {payment_intents_added}")
        print(f"Initial database count: {initial_count}")
        print(f"Final database count: {final_count}")
        print(f"Net change in database: {final_count - initial_count}")
        
    except Exception as e:
        print(f"Error in sync_charges: {str(e)}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

# Sync payment methods from Stripe to PostgreSQL
def sync_payment_methods():
    print("Syncing payment methods...")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Get current count before sync
        cursor.execute("SELECT COUNT(*) FROM payment_methods")
        initial_count = cursor.fetchone()[0]
        print(f"Current records in database: {initial_count}")
        
        # Get the latest created timestamp from the database
        cursor.execute("SELECT MAX(created) FROM payment_methods")
        last_created = cursor.fetchone()[0]
        
        params = {'limit': 100}
        if last_created:
            # Convert datetime to Unix timestamp
            last_created_timestamp = int(last_created.timestamp())
            print(f"Fetching payment methods created after: {last_created} (timestamp: {last_created_timestamp})")
            params['created'] = {'gt': last_created_timestamp}
        else:
            print("No existing records found, fetching all payment methods")
        
        has_more = True
        starting_after = None
        total_count = 0
        page_count = 0
        new_records = 0
        updated_records = 0
        failed_records = 0
        
        while has_more:
            try:
                if starting_after:
                    params['starting_after'] = starting_after
                    
                payment_methods = stripe.PaymentMethod.list(**params)
                page_count += 1
                count = 0
                
                if not payment_methods.data:
                    print(f"No payment methods found in page {page_count}")
                    break
                
                for payment_method in payment_methods.auto_paging_iter():
                    try:
                        # Start a new transaction for each payment method
                        conn.rollback()  # Ensure we start fresh
                        cursor = conn.cursor()
                        
                        created_date = datetime.datetime.fromtimestamp(payment_method.created)
                        customer_id = payment_method.customer if hasattr(payment_method, 'customer') else None
                        
                        # Check if record exists
                        cursor.execute("SELECT id FROM payment_methods WHERE id = %s", (payment_method.id,))
                        exists = cursor.fetchone() is not None
                        
                        cursor.execute(
                            """
                            INSERT INTO payment_methods (id, customer_id, type, created, metadata, data)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (id) DO UPDATE
                            SET customer_id = EXCLUDED.customer_id,
                                type = EXCLUDED.type,
                                created = EXCLUDED.created,
                                metadata = EXCLUDED.metadata,
                                data = EXCLUDED.data
                            """,
                            (
                                payment_method.id,
                                customer_id,
                                payment_method.type,
                                created_date,
                                psycopg2.extras.Json(payment_method.metadata),
                                psycopg2.extras.Json(payment_method)
                            )
                        )
                        
                        if exists:
                            updated_records += 1
                        else:
                            new_records += 1
                            
                        count += 1
                        total_count += 1
                        
                    except Exception as e:
                        print(f"Error processing payment method {payment_method.id}: {str(e)}")
                        print(f"Data content: {data}")
                        failed_records += 1
                        conn.rollback()
                        continue
                
                # Commit after each page
                conn.commit()
                print(f"Page {page_count}: Processed {count} payment methods (New: {new_records}, Updated: {updated_records}, Failed: {failed_records})")
                
                has_more = payment_methods.has_more
                if has_more and len(payment_methods.data) > 0:
                    starting_after = payment_methods.data[-1].id
                else:
                    has_more = False
                    
                # Update last sync timestamp after each successful page
                update_last_sync('payment_methods')
                
            except Exception as e:
                print(f"Error processing page {page_count}: {str(e)}")
                conn.rollback()
                raise
        
        # Get final count
        cursor.execute("SELECT COUNT(*) FROM payment_methods")
        final_count = cursor.fetchone()[0]
        
        print("\nPayment Methods Sync Summary:")
        print(f"Total pages processed: {page_count}")
        print(f"Total records processed: {total_count}")
        print(f"New records added: {new_records}")
        print(f"Records updated: {updated_records}")
        print(f"Records failed: {failed_records}")
        print(f"Initial database count: {initial_count}")
        print(f"Final database count: {final_count}")
        print(f"Net change in database: {final_count - initial_count}")
        
    except Exception as e:
        print(f"Error in sync_payment_methods: {str(e)}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

# Main function to run the sync
def main():
    print("Starting Stripe to PostgreSQL sync...")
    
    # Setup database tables
    setup_database()
    
    # Sync all resources
    sync_customers()
    sync_products()
    sync_prices()
    sync_subscriptions()  # Make sure this runs before invoices
    sync_invoices()
    sync_payment_intents()
    sync_charges()
    sync_payment_methods()
    
    print("Sync completed successfully!")

if __name__ == "__main__":
    main()