import subprocess
from datetime import datetime, date
import logging
import paramiko
import os
import sys
import pandas as pd
import io

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

current_date_str = datetime.now().strftime('%Y%m%d')

# --- Configuration ---
HOSTNAME = 'connecta.uvesolutions.com'
USERNAME = 'Agent-16042-UMY'
PASSWORD = 'Sd5n3MEQ)QF+'  # WARNING: Hardcoded password for example only.
LOCAL_PATH = r'C:\Unisales'
REMOTE_PATH = '/MYSG'
FILENAME = f'Unisales{current_date_str}.csv'



def run_sqlplus_script_from_file(script_filename="Unisales.txt"):
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


def transfer_file_sftp():
    """
    Connects to an SFTP server and transfers a single file.
    """
    sftp = None
    transport = None
    try:
        # Create a transport object to manage the connection
        print(f"Connecting to {HOSTNAME}...")
        transport = paramiko.Transport((HOSTNAME, 22))

        # Connect using username and password
        transport.connect(username=USERNAME, password=PASSWORD)
        print("Connection successful.")

        # Create the SFTP client
        sftp = paramiko.SFTPClient.from_transport(transport)
        print("SFTP client created.")

        # Change to the remote directory
        print(f"Changing remote directory to {REMOTE_PATH}")
        sftp.chdir(REMOTE_PATH)

        # Build the full local file path
        local_filepath = os.path.join(LOCAL_PATH, FILENAME)
        print(f"Attempting to transfer '{local_filepath}'...")

        # Transfer the file
        sftp.put(local_filepath, FILENAME)
        print("File transferred successfully!")

        return True

    except FileNotFoundError:
        print(f"Error: The local file '{local_filepath}' was not found.", file=sys.stderr)
        return False
    except paramiko.AuthenticationException:
        print("Error: Authentication failed. Please check your username and password.", file=sys.stderr)
        return False
    except paramiko.SSHException as e:
        print(f"Error: SSH connection failed. Details: {e}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        return False
    finally:
        # Ensure the connections are closed
        if sftp:
            sftp.close()
            print("SFTP connection closed.")
        if transport:
            transport.close()
            print("Transport connection closed.")


# Run the script
if __name__ == "__main__":
    run_sqlplus_script_from_file(r"C:\Users\USER\PycharmProjects\EmailOracle\Unisales1.txt")
    current_date_str = datetime.now().strftime('%yyyy%mm%dd')

    """with open(r"C:\Unisales\Unisales.csv", 'r') as f:
        first_line = f.readline()
        num_columns = len(first_line.split(','))
        print(f"Number of columns: {num_columns}")"""





    if transfer_file_sftp():
        print("\nSuccess")
    else:
        print("\nError")
