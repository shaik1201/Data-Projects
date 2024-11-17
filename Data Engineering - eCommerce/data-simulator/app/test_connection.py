import mysql.connector
from mysql.connector import Error
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_mysql_connection():
    try:
        connection = mysql.connector.connect(
            host="mysql",
            database="ecommerce",
            user="spark",
            password="spark123"
        )
        
        if connection.is_connected():
            db_info = connection.get_server_info()
            logger.info(f"Connected to MySQL Server version {db_info}")
            
            cursor = connection.cursor()
            cursor.execute("SELECT DATABASE();")
            record = cursor.fetchone()
            logger.info(f"You're connected to database: {record[0]}")
            
            # Test table access
            cursor.execute("DESCRIBE sales_analytics;")
            logger.info("Table structure:")
            for row in cursor.fetchall():
                logger.info(row)
            
            # Insert test record
            cursor.execute("""
                INSERT INTO sales_analytics 
                (window_start, window_end, category, total_sales, avg_price, num_transactions)
                VALUES 
                (NOW(), NOW(), 'TEST', 100.0, 50.0, 2)
                ON DUPLICATE KEY UPDATE
                total_sales = total_sales + 100.0,
                num_transactions = num_transactions + 2;
            """)
            connection.commit()
            logger.info("Successfully inserted test record")
            
            # Verify data
            cursor.execute("SELECT * FROM sales_analytics WHERE category = 'TEST';")
            logger.info("Test record:")
            for row in cursor.fetchall():
                logger.info(row)

    except Error as e:
        logger.error(f"Error while connecting to MySQL: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            logger.info("MySQL connection is closed")

if __name__ == "__main__":
    test_mysql_connection()