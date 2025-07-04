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
        query = """
        SET ECHO OFF
SET TERMOUT off
set linesize 10000
set underline off
set headsep off
set colsep ,
set pagesize 4000
set trimspool on
set embedded on
set feedback off
column dcol new_value mydate noprint
select to_char(sysdate,'YYYYMMDD') dcol from dual;
column ItemCode format 999999999999 heading "Item Code"
column ItemDesc format a100 heading "ItemDesc"
COLUMN Productcode1 FORMAT 99999999999 HEADING "Productcode1"
column LotNUm format a50 heading "LotNUm"
column Qty format 999999999 heading "Qty"
column Cost format 999999999 heading "Cost"
column SellingPrice format 999999999 heading "SellingPrice"
column Value format 999999999 heading "Value"
column Ware format a15 heading "Ware"
column SalesChannel format a15 heading "SalesChannel"
column BusChannel format a15 heading "BusChannel"
column ProdChannel format a15 heading "ProdChannel"
column StkType format a15 heading "StkType"
column UOm format a15 heading "UOm"
column ditrib format a15 heading "ditrib"
COLUMN CTN FORMAT 99999 HEADING "CTNQTY"
column EXPDATE format DATE heading "EXPDATE"
column onhANDdATE format DATE heading "onhANDdATE"
column dcol new_value mydate noprint
SPOOL C:/NESTLE/NSBXTPLSH.&mydate..csv
SELECT
'LSH',
to_char(sysdate,'YYYYMMDD') onhANDdATE,
'5052026   ' ditrib,
substr(mast.segment1,3,30) "ItemDesc",
sum(QTY.primary_transaction_quantity) Qty,
sum(QTY.primary_transaction_quantity)*CST.item_cost Value,
0 Cost,
0 SellingPrice,
'M' Ware ,
'11' SalesChannel,
'13' BusChannel,
' ' ProdChannel,
' ' StkType,
substr(mast.segment1,3,30) "Item Code",
CASE  
  WHEN INSTR(mast.description, '-') > 0 THEN
    SUBSTR(mast.description, 1, LEAST(INSTR(mast.description, '-') - 1, 80))
  ELSE 
    SUBSTR(mast.description, 1, 80)
END AS "Item Description",
CASE 
  WHEN INSTR(mast.description, '-') > 0 THEN 
    SUBSTR(mast.description, INSTR(mast.description, '-') + 1, 80)
  ELSE 
    SUBSTR(mast.description, 1, 80)
END AS "Combine Packing",
to_char(sysdate+360,'YYYYMMDD') EXPDATE
FROM 
Mtl_System_Items Mast,
Mtl_Onhand_Quantities_Detail Qty,
cst_item_costs CST 
WHERE
Qty.Inventory_Item_Id = Mast.Inventory_Item_Id
and mast.inventory_item_id= CST.inventory_item_id
And Mast.Organization_Id='82'
and cst.Organization_Id='82'
AND MAST.SEGMENT1 LIKE 'XN%'
group by MAST.attribute8  ,Mast.segment1 ,MAST.PRIMARY_UOM_CODE,CST.item_cost,mast.description
/
        """

        try:
            logger.info("Executing sales query...")
            df = pd.read_sql(query, connection)
            logger.info(f"Query executed successfully. Retrieved {len(df)} records")
            return df

        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise

    def export_to_excel(self, df, filename=None):
        """Export DataFrame to Excel file"""
        try:
            if filename is None:
                current_date = datetime.now().strftime('%Y%m%d')
                filename = f"XLSALES_{current_date}.xlsx"

            # Create directory if it doesn't exist
            output_dir = "C:/XLSALES"
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)

            filepath = os.path.join(output_dir, filename)

            # Create Excel writer object
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                # Write data to Excel
                df.to_excel(writer, sheet_name='Sales Report', index=False)

                # Get workbook and worksheet
                workbook = writer.book
                worksheet = writer.sheets['Sales Report']

                # Auto-adjust column widths
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width

                # Add header formatting
                from openpyxl.styles import Font, PatternFill
                header_font = Font(bold=True)
                header_fill = PatternFill(start_color="CCCCCC", end_color="CCCCCC", fill_type="solid")

                for cell in worksheet[1]:
                    cell.font = header_font
                    cell.fill = header_fill

            logger.info(f"Excel file created successfully: {filepath}")
            return filepath

        except Exception as e:
            logger.error(f"Error creating Excel file: {e}")
            raise

    def send_email(self, excel_filepath, recipient_list,cc_recipient_list):
        """Send email with Excel attachment"""
        try:
            # Create message
            msg = MIMEMultipart()
            msg['From'] = self.email_config['sender_email']
            msg['To'] = ', '.join(recipient_list)
            msg['Subject'] = f"XL Sales Report - {datetime.now().strftime('%Y-%m-%d')}"
            #msg['cc'] = ', '.join(cc_recipient_list)

            # Email body
            body = f"""
            Dear Team,

            Please find attached the XL Sales Report for {datetime.now().strftime('%B %Y')}.

            Report Details:
            - Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            - File: {os.path.basename(excel_filepath)}

            Best regards,
            Sales Reporting System
            """

            msg.attach(MIMEText(body, 'plain'))

            # Attach Excel file
            with open(excel_filepath, "rb") as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header(
                    'Content-Disposition',
                    f'attachment; filename= {os.path.basename(excel_filepath)}'
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

            if df.empty:
                logger.warning("No data found for the current period")
                return

            # Export to Excel
            excel_filepath = self.export_to_excel(df)

            # Send email
            #self.send_email(excel_filepath, recipient_list,cc_recipient)

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
    recipients = [
        'customer_service@lkk.com',
        'tony.loh@my.lkk.com',
        'christy.lau@my.lkk.com',
        'phillip.tan@my.lkk.com']


    cc_recipients = ['elvis@lshworld.com',
                     'lissa@lshworld.com',
                     'taiyan@lshworld.com',
                        'shell_dc@lshworld.com',
                     'amore@lshworld.com','annie@lshworld.com']



    # Create report generator instance
    report_generator = XLSalesReportGenerator(db_config, email_config)

    # Generate and send report
    try:
        report_generator.generate_and_send_report(recipients,cc_recipients)
        print("Report generated and sent successfully!")
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