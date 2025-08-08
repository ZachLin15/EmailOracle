import subprocess
import os
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime, date
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run_sqlplus_script_from_file(script_filename="XESALES.txt"):
    """
    Writes the provided SQL*Plus script content to a file and then executes it
    using sqlplus via subprocess.

    Args:
        script_content (str): The full SQL*Plus script as a string.
        script_filename (str): The name of the file to save the script to.
    """
    # Create the script file


    # Construct the command to run sqlplus
    # /nolog: Starts sqlplus without logging in initially (script handles CONNECT)
    # @script_filename: Executes the commands in the specified script file
    command = ["sqlplus", "/nolog", f"@{script_filename}"]

    print(f"Executing command: {' '.join(command)}")

    try:
        # Execute the command
        # capture_output=True: Captures stdout and stderr
        # text=True: Decodes stdout/stderr as text
        # check=False: Prevents Python from raising an exception for non-zero exit codes
        result = subprocess.run(command, capture_output=True, text=True, check=False)

        # Print SQL*Plus output
        print("\n--- SQL*Plus Standard Output ---")
        print(result.stdout)

        print("\n--- SQL*Plus Standard Error ---")
        print(result.stderr)

        print(f"\n--- SQL*Plus Return Code: {result.returncode} ---")

        if result.returncode == 0:
            print("\nSQL*Plus script executed successfully.")
            # The CSV file will be generated at C:/NESTLE/NCMXTPLSH_YYYYMMDD.csv
            print("Check the specified SPOOL location for the generated CSV file:")
            print("C:/XESALES/ZHSALES_YYYYMMDD.csv (YYYYMMDD will be current date)")
        else:
            print("\nSQL*Plus script execution failed or encountered warnings/errors.")
            print("Please review the 'SQL*Plus Standard Error' and 'Standard Output' above for details.")

    except FileNotFoundError:
        print(f"\nError: 'sqlplus' command not found.")
        print("Please ensure Oracle Client is installed and 'sqlplus' is in your system's PATH environment variable.")
    except Exception as e:
        print(f"\nAn unexpected error occurred during subprocess execution: {e}")
    finally:
        # Optional: Clean up the script file after execution
        # try:
        #     os.remove(script_filename)
        #     print(f"\nCleaned up script file: {script_filename}")
        # except OSError as e:
        #     print(f"Error removing script file {script_filename}: {e}")
        pass # Keeping the file for debugging purposes



# Run the script
if __name__ == "__main__":
    run_sqlplus_script_from_file(r"C:\Users\USER\PycharmProjects\EmailOracle\ZHSALES.txt")
    current_date_str = datetime.now().strftime('%y%m%d')

    excel_filepath = f"C:\\ZHSALES\\ZHSALES.csv"

    # Email configuration
    email_config = {
        'smtp_server': 'smtp.office365.com',  # Change to your SMTP server
        'smtp_port': 587,
        'sender_email': 'admin1@lshworld.com',
        'sender_password': 'dpvqmxwsrxvxmbvr',  # Use app password for Gmail
        'use_tls': True
    }
    recipients = [  'Huslinda.husin@allexcel.com.my',
                    'fariz.noryatim@allexcel.com.my',
                    'huimin@allswelltrading.com.sg',
                    'serene@allswelltrading.com.sg',
                    'cheryl.tan@allswelltrading.com.sg',
                    'Jimmy.choo@allswelltrading.com.sg',
                    'gary.teo@allswelltrading.com.sg',
                    'beeluan.chia@allswelltrading.com.sg',
                    'ariella.lee@allswelltrading.com.sg',
                    'elaine@lshworld.com',
                    'cy.lee@lshworld.com',
                    'sabrin.fong@allexcel.com.my',
                    'meifung.chong@allexcel.com.my',
                    'paul.wee@allswelltrading.com.sg',
                    'diana.khalid@allswelltrading.com.sg',
                    'caroltoh@allswelltrading.com.sg',
                    'lucia@allswelltrading.com.sg',
                    'zhenglin@limsianghuat.com',
                    'amore@lshworld.com'

                  ]

    try:
        # Create message
        msg = MIMEMultipart()
        msg['From'] = email_config['sender_email']
        msg['To'] = ', '.join(recipients)
        msg['Subject'] = f" Allwell Sales Report - {datetime.now().strftime('%Y-%m-%d')}"
        # msg['cc'] = ', '.join(cc_recipient_list)

        # Email body
        body = f"""
        Dear Team,

        Please find attached the Allwell Sales Report for {datetime.now().strftime('%B %Y')}.

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
        server = smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port'])

        if email_config.get('use_tls', True):
            server.starttls()

        server.login(email_config['sender_email'], email_config['sender_password'])
        server.send_message(msg)
        server.quit()

        logger.info(f"Email sent successfully to {len(recipients)} recipients")

    except Exception as e:
        logger.error(f"Error sending email: {e}")
        raise
    os.remove(excel_filepath)
