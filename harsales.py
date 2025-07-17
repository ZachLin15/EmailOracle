import subprocess
import os

def run_sqlplus_script_from_file(script_filename="harcust.txt"):
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
            print("C:/NESTLE/NCMXTPLSH_YYYYMMDD.csv (YYYYMMDD will be current date)")
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
    run_sqlplus_script_from_file(r"C:\Users\USER\PycharmProjects\EmailOracle\HARCUST.txt")