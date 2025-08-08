import cx_Oracle as oracledb
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime, date
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class XLSalesReportGenerator:
    def __init__(self, db_config, email_config):
        """
        Initialize the report generator

        Args:
            db_config (dict): Database connection configuration
            email_config (dict): Email configuration
        """
        self.db_config = db_config
        self.email_config = email_config

    def get_database_connection(self):
        """Establish database connection using oracledb"""
        try:
            logger.info("Attempting database connection...")

            # Use your specific connection parameters
            connection = oracledb.connect(
                user=self.db_config['username'],
                password=self.db_config['password'],
                dsn=self.db_config['dsn'],
                encoding=self.db_config.get('encoding', 'UTF-8')
            )

            logger.info("Database connection established successfully")
            return connection

        except oracledb.Error as e:
            logger.error(f"Database connection error: {e}")
            raise

    def execute_sales_query(self, connection):
        """Execute the sales report query"""

        # Convert the original SQL query to proper format
        query = '''
SELECT
    'LSH' AS Channel, -- "LSH" as a fixed string
    TO_CHAR(SYSDATE, 'YYYYMMDD') AS onhANDdATE,
    '5052026' AS ditrib, -- Removed extra spaces
    SUBSTR(mast.segment1, 3, 30) AS ItemCode, -- Renamed from 'ItemDesc' in SELECT for consistency with COLUMN def
    SUBSTR(mast.segment1, 3, 30) AS Productcode1, -- Mapping to Productcode1 based on your COLUMN def
    SUM(QTY.primary_transaction_quantity) AS Qty,
    SUM(QTY.primary_transaction_quantity) * CST.item_cost AS Value,
    0 AS Cost,
    0 AS SellingPrice,
    'M' AS Ware,
    '11' AS SalesChannel,
    '13' AS BusChannel,
    ' ' AS ProdChannel,
    ' ' AS StkType,
    -- Note: 'UOm' and 'CTN' were defined as columns but not selected in your SQL.
    -- If needed, you'll have to add them to the SELECT statement from your tables.
    CASE
        WHEN INSTR(mast.description, '-') > 0 THEN
            SUBSTR(mast.description, 1, LEAST(INSTR(mast.description, '-') - 1, 80))
        ELSE
            SUBSTR(mast.description, 1, 80)
    END AS ItemDescription, -- "Item Description" heading
    CASE
        WHEN INSTR(mast.description, '-') > 0 THEN
            SUBSTR(mast.description, INSTR(mast.description, '-') + 1, 80)
        ELSE
            SUBSTR(mast.description, 1, 80)
    END AS CombinePacking, -- "Combine Packing" heading
    TO_CHAR(SYSDATE + 360, 'YYYYMMDD') AS EXPDATE
FROM
    Mtl_System_Items Mast,
    Mtl_Onhand_Quantities_Detail Qty,
    cst_item_costs CST
WHERE
    Qty.Inventory_Item_Id = Mast.Inventory_Item_Id
    AND mast.inventory_item_id = CST.inventory_item_id
    AND Mast.Organization_Id = 82 -- Numbers should not be in quotes for direct comparison
    AND cst.Organization_Id = 82
    AND MAST.SEGMENT1 LIKE 'XN%'
GROUP BY
    MAST.attribute8, Mast.segment1, MAST.PRIMARY_UOM_CODE, CST.item_cost, mast.description
'''

        try:
            current_date_str = datetime.now().strftime('%Y%m%d')
            filename = f'C:/NESTLE/NSBXTPLSH_{current_date_str}.csv'
            logger.info("Executing sales query...")
            df = pd.read_sql(query, connection)
            logger.info(f"Query executed successfully. Retrieved {len(df)} records")
            string_columns = df.select_dtypes(include='object').columns
            for col in string_columns:
                df[col] = df[col].astype(str).str.rstrip().replace('None',
                                                                   None)  # Replace 'None' string back to actual None
            df.to_csv(
                filename,
                sep=',',  # Use tab as separator
                index=False,  # Include the DataFrame index as the first column
                header=True,  # Include column headers
                encoding='utf-8'  # Specify encoding
            )
            return ''

        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise



    def send_email(self, recipient_list,cc_recipient_list):
        """Send email with Excel attachment"""
        current_date = datetime.now().strftime('%Y%m%d')

        attachment_filepaths = [
            f"C:/NESTLE/NSTXTPLSH_{current_date}.csv",  # Example XLSX file 1
            f"C:/NESTLE/NSBXTPLSH_{current_date}.csv",  # Example XLSX file 2
            f"C:/NESTLE/NCMXTPLSH_{current_date}.csv",  # Another XLSX file 3
        ]
        try:
            # Create message
            msg = MIMEMultipart()
            msg['From'] = self.email_config['sender_email']
            msg['To'] = ', '.join(recipient_list)
            msg['Subject'] = f"Nestle Sales Report - {datetime.now().strftime('%Y-%m-%d')}"
            #msg['cc'] = ', '.join(cc_recipient_list)

            # Email body
            body = f"""
            Dear Team,

            Please find attached the Nestle Sales Report for {datetime.now().strftime('%B %Y')}.

            Report Details:
            - Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            - File: {attachment_filepaths}

            Best regards,
            Sales Reporting System
            """

            msg.attach(MIMEText(body, 'plain'))

            # Attach Excel file
            for filepath in attachment_filepaths:
                with open(filepath, "rb") as attachment:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment.read())
                    encoders.encode_base64(part)
                    part.add_header(
                        'Content-Disposition',
                        f'attachment; filename= {os.path.basename(filepath)}'
                    )
                    msg.attach(part)

            # Send email
            server = smtplib.SMTP(self.email_config['smtp_server'], self.email_config['smtp_port'])

            if self.email_config.get('use_tls', True):
                server.starttls()

            server.login(self.email_config['sender_email'], self.email_config['sender_password'])
            server.send_message(msg)
            server.quit()

            logger.info(f"Email sent successfully to {len(recipient_list)} recipients")

        except Exception as e:
            logger.error(f"Error sending email: {e}")
            raise

    def generate_and_send_report(self, recipient_list,cc_recipient):
        """Main method to generate report and send email"""
        connection = None
        try:
            # Connect to database
            connection = self.get_database_connection()

            # Execute query
            df = self.execute_sales_query(connection)



            # Export to Excel
            #excel_filepath = self.export_to_excel(df)

            # Send email
            self.send_email(recipient_list,cc_recipient)

            logger.info("Report generation and email sending completed successfully")

        except Exception as e:
            logger.error(f"Error in report generation process: {e}")
            raise
        finally:
            if connection:
                connection.close()
                logger.info("Database connection closed")


# Configuration and usage example
if __name__ == "__main__":
    # Database configuration using your connection parameters
    db_config = {
        'username': 'apps',
        'password': 'apps',
        'dsn': '192.168.200.179/erpp',
        'encoding': 'UTF-8'
    }

    # Email configuration
    email_config = {
        'smtp_server': 'smtp.office365.com',  # Change to your SMTP server
        'smtp_port': 587,
        'sender_email': 'admin1@lshworld.com',
        'sender_password': 'dpvqmxwsrxvxmbvr',  # Use app password for Gmail
        'use_tls': True
    }



    # Recipients list
    cc_recipients = [
        'zhenglin@limsianghuat.com']


    recipients = ['BoonHua.Ong@SG.nestle.com',
                     'Micky@lshworld.com',
                     'Lily@lshworld.com',
                     'amore@lshworld.com',
                     'annie@lshworld.com',
                     'SGSINBusinessSolutionsSupport@internal.nestle.com',
                     'Steven.Tan@SG.nestle.com',
                     'Adrian.Ang@sg.nestle.com',
                    'shell_dc@lshworld.com']






    # Create report generator instance
    report_generator = XLSalesReportGenerator(db_config, email_config)

    # Generate and send report
    try:
        report_generator.generate_and_send_report(recipients,cc_recipients)
        print("Report generated and sent successfully!")

        #REMOVE ALL SUCCESFULL FILES
        current_date = datetime.now().strftime('%Y%m%d')
        os.remove(f"C:/NESTLE/NSTXTPLSH_{current_date}.csv")
        os.remove(f"C:/NESTLE/NSBXTPLSH_{current_date}.csv")
        os.remove(f"C:/NESTLE/NCMXTPLSH_{current_date}.csv")
        print("3files are deleted !")


    except Exception as e:
        print(f"Error: {e}")




# Test database connection function
def test_database_connection():
    """Test the database connection"""
    db_config = {
        'username': 'apps',
        'password': 'apps',
        'dsn': '192.168.200.179/erpp',
        'encoding': 'UTF-8'
    }

    try:
        logger.info("Testing database connection...")
        connection = oracledb.connect(
            user=db_config['username'],
            password=db_config['password'],
            dsn=db_config['dsn'],
            encoding=db_config['encoding']
        )

        print("✓ Database connection - SUCCESS")

        # Test a simple query
        cursor = connection.cursor()
        cursor.execute("SELECT SYSDATE FROM DUAL")
        result = cursor.fetchone()
        print(f"  Database date: {result[0]}")
        cursor.close()
        connection.close()

        return True

    except Exception as e:
        print(f"✗ Database connection - FAILED: {e}")
        return False


# Alternative method for testing without email
def generate_report_only():
    """Generate report without sending email (for testing)"""
    db_config = {
        'username': 'apps',
        'password': 'apps',
        'dsn': '192.168.200.179/erpp',
        'encoding': 'UTF-8'
    }

    email_config = {}  # Empty for testing

    report_generator = XLSalesReportGenerator(db_config, email_config)

    try:
        connection = report_generator.get_database_connection()
        df = report_generator.execute_sales_query(connection)
        excel_filepath = report_generator.export_to_excel(df)
        connection.close()

        print(f"Report generated successfully: {excel_filepath}")
        return excel_filepath

    except Exception as e:
        print(f"Error: {e}")
        return None


# Requirements.txt content (install these packages):
"""
cx_Oracle>=8.3.0
pandas>=1.5.0
openpyxl>=3.0.0
"""