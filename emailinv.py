# Import the necessary libraries
import smtplib
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
import os
import pandas as pd


def send_email(to_address, subject, body,excel_filepath):
    """
    Sends an email using the provided recipient, subject, and body.

    Args:
        to_address (str): The recipient's email address.
        subject (str): The subject line of the email.
        body (str): The main content of the email.
    """

    email_config = {
        'smtp_server': 'smtp.office365.com',  # Change to your SMTP server
        'smtp_port': 587,
        'sender_email': 'admin1@lshworld.com',
        'sender_password': 'dpvqmxwsrxvxmbvr',  # Use app password for Gmail
        'use_tls': True
    }

    # Create the email message
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = email_config['sender_email']
    msg['To'] = to_address
    msg['Cc'] = "accounts1@lshworld.com"



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
    server = smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port'])

    server.starttls()  # remove this line after change back email server, also use line 712

    try:
        # Connect to the SMTP server (for Gmail, the address and port are below)
        # You may need to change this depending on your email provider.
        server.login(email_config['sender_email'], email_config['sender_password'])
        server.send_message(msg)
        server.quit()

        print(f"\nEmail successfully sent to {to_address}")
    except Exception as e:
        print(f"\nAn error occurred while sending the email to {to_address}: {e}")


# --- Main script execution ---
# This part assumes you have already run the previous script and have a file like 'extracted_data.xlsx'
extracted_file = "extracted_data.xlsx"

if os.path.exists(extracted_file):
    try:
        # Read the Excel file into a pandas DataFrame
        df = pd.read_excel(extracted_file)

        # Iterate through each row in the DataFrame
        for index, row in df.iterrows():
            customer_email = row['email']
            customer_company = row['company']
            customer_file = os.path.join(r"X:\PRINTINVOICE\TEST_INPUT\pdf4\filtered",row['file_name'])

            # Customize the subject and body of the email
            email_subject = f"Updated Invoice for {customer_company}"
            email_body = f"""Dear Sir/Madam,

Please find the updated invoice for your recent order which now includes the Goods and Services Tax (GST).

We sincerely apologize for this inconvenience and thank you for your understanding. 

This is an automated email.

Best regards,
Lim Siang Huat Pte Ltd"""

            # Call the function to send the email
            send_email(customer_email, email_subject, email_body,customer_file)

    except Exception as e:
        print(f"\nError reading the Excel file or sending emails: {e}")
else:
    print(f"Error: The file '{extracted_file}' was not found. Please ensure the path is correct.")

