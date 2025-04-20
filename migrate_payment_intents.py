import os
import psycopg2
import psycopg2.extras
import json
import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv("secrets.env")

# PostgreSQL configuration
pg_config = {
    'host': os.getenv('PG_HOST', 'localhost'),
    'port': os.getenv('PG_PORT', '5432'),
    'dbname': 'stripe',
    'user': os.getenv('PG_USER', 'postgres'),
    'password': os.getenv('PG_PASSWORD', '')
}

def get_db_connection():
    """Get a database connection."""
    config = {k: v for k, v in pg_config.items() if v}
    return psycopg2.connect(**config)

def setup_payment_intents_columns():
    """Create missing columns in the payment_intents table."""
    print("Setting up payment_intents columns...")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # List of columns to add with their data types
        columns_to_add = [
            ('amount', 'INTEGER'),
            ('object', 'TEXT'),
            ('review', 'TEXT'),
            ('source', 'TEXT'),
            ('status', 'TEXT'),
            ('created', 'TIMESTAMP'),
            ('currency', 'TEXT'),
            ('customer_id', 'TEXT'),
            ('livemode', 'BOOLEAN'),
            ('metadata', 'JSONB'),
            ('shipping', 'JSONB'),
            ('processing', 'JSONB'),
            ('application', 'TEXT'),
            ('canceled_at', 'TIMESTAMP'),
            ('description', 'TEXT'),
            ('next_action', 'JSONB'),
            ('on_behalf_of', 'TEXT'),
            ('client_secret', 'TEXT'),
            ('latest_charge', 'TEXT'),
            ('receipt_email', 'TEXT'),
            ('transfer_data', 'JSONB'),
            ('amount_details', 'JSONB'),
            ('capture_method', 'TEXT'),
            ('payment_method', 'TEXT'),
            ('transfer_group', 'TEXT'),
            ('amount_received', 'INTEGER'),
            ('amount_capturable', 'INTEGER'),
            ('last_payment_error', 'JSONB'),
            ('setup_future_usage', 'TEXT'),
            ('cancellation_reason', 'TEXT'),
            ('confirmation_method', 'TEXT'),
            ('payment_method_types', 'TEXT[]'),
            ('statement_descriptor', 'TEXT'),
            ('application_fee_amount', 'INTEGER'),
            ('payment_method_options', 'JSONB'),
            ('automatic_payment_methods', 'JSONB'),
            ('statement_descriptor_suffix', 'TEXT'),
            ('payment_method_configuration_details', 'JSONB')
        ]
        
        # Get existing columns
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'payment_intents'
        """)
        existing_columns = {row[0] for row in cursor.fetchall()}
        
        # Add missing columns
        for column_name, data_type in columns_to_add:
            if column_name not in existing_columns:
                print(f"Adding column {column_name}...")
                cursor.execute(f"""
                    ALTER TABLE payment_intents 
                    ADD COLUMN IF NOT EXISTS {column_name} {data_type}
                """)
        
        conn.commit()
        print("Column setup completed successfully")
        
    except Exception as e:
        print(f"Error setting up columns: {str(e)}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

def migrate_payment_intents():
    print("Starting payment intents migration...")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Get total count of records
        cursor.execute("SELECT COUNT(*) FROM payment_intents")
        total_records = cursor.fetchone()[0]
        print(f"Total records to migrate: {total_records}")
        
        # Process records in batches
        batch_size = 1000
        processed = 0
        updated = 0
        failed = 0
        
        while processed < total_records:
            try:
                # Fetch a batch of records
                cursor.execute(
                    """
                    SELECT id, data 
                    FROM payment_intents 
                    ORDER BY id 
                    LIMIT %s OFFSET %s
                    """,
                    (batch_size, processed)
                )
                
                batch = cursor.fetchall()
                if not batch:
                    break
                    
                for record_id, data in batch:
                    try:
                        # Extract fields with proper type conversion
                        created_date = datetime.datetime.fromtimestamp(data.get('created', 0))
                        canceled_at = datetime.datetime.fromtimestamp(data.get('canceled_at', 0)) if data.get('canceled_at') else None
                        
                        # Update the record with extracted fields
                        cursor.execute(
                            """
                            UPDATE payment_intents SET
                                amount = %s,
                                status = %s,
                                created = %s,
                                currency = %s,
                                customer_id = %s,
                                livemode = %s,
                                metadata = %s,
                                shipping = %s,
                                processing = %s,
                                application = %s,
                                canceled_at = %s,
                                description = %s,
                                next_action = %s,
                                on_behalf_of = %s,
                                client_secret = %s,
                                latest_charge = %s,
                                receipt_email = %s,
                                transfer_data = %s,
                                amount_details = %s,
                                capture_method = %s,
                                payment_method = %s,
                                transfer_group = %s,
                                amount_received = %s,
                                amount_capturable = %s,
                                last_payment_error = %s,
                                setup_future_usage = %s,
                                cancellation_reason = %s,
                                confirmation_method = %s,
                                payment_method_types = %s,
                                statement_descriptor = %s,
                                application_fee_amount = %s,
                                payment_method_options = %s,
                                automatic_payment_methods = %s,
                                statement_descriptor_suffix = %s,
                                payment_method_configuration_details = %s
                            WHERE id = %s
                            """,
                            (
                                data.get('amount'),
                                data.get('status'),
                                created_date,
                                data.get('currency'),
                                data.get('customer'),
                                data.get('livemode'),
                                psycopg2.extras.Json(data.get('metadata', {})),
                                psycopg2.extras.Json(data.get('shipping', {})),
                                psycopg2.extras.Json(data.get('processing', {})),
                                data.get('application'),
                                canceled_at,
                                data.get('description'),
                                psycopg2.extras.Json(data.get('next_action', {})),
                                data.get('on_behalf_of'),
                                data.get('client_secret'),
                                data.get('latest_charge'),
                                data.get('receipt_email'),
                                psycopg2.extras.Json(data.get('transfer_data', {})),
                                psycopg2.extras.Json(data.get('amount_details', {})),
                                data.get('capture_method'),
                                data.get('payment_method'),
                                data.get('transfer_group'),
                                data.get('amount_received'),
                                data.get('amount_capturable'),
                                psycopg2.extras.Json(data.get('last_payment_error', {})),
                                data.get('setup_future_usage'),
                                data.get('cancellation_reason'),
                                data.get('confirmation_method'),
                                data.get('payment_method_types'),
                                data.get('statement_descriptor'),
                                data.get('application_fee_amount'),
                                psycopg2.extras.Json(data.get('payment_method_options', {})),
                                psycopg2.extras.Json(data.get('automatic_payment_methods', {})),
                                data.get('statement_descriptor_suffix'),
                                psycopg2.extras.Json(data.get('payment_method_configuration_details', {})),
                                record_id
                            )
                        )
                        updated += 1
                        
                    except Exception as e:
                        print(f"Error processing record {record_id}: {str(e)}")
                        print(f"Data content: {data}")
                        failed += 1
                        # Rollback the current transaction
                        conn.rollback()
                        # Start a new transaction
                        cursor = conn.cursor()
                        continue
                
                # Commit after each batch
                conn.commit()
                processed += len(batch)
                print(f"Processed {processed}/{total_records} records (Updated: {updated}, Failed: {failed})")
                
            except Exception as e:
                print(f"Error processing batch: {str(e)}")
                conn.rollback()
                # Start a new transaction
                cursor = conn.cursor()
                continue
        
        print("\nMigration Summary:")
        print(f"Total records processed: {processed}")
        print(f"Records successfully updated: {updated}")
        print(f"Records failed: {failed}")
        
    except Exception as e:
        print(f"Error during migration: {str(e)}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    setup_payment_intents_columns()
    migrate_payment_intents() 