'''This project integrates Python and SQL. I will be creating and managing a database, building a schema, and performing various SQL operations including quiries with joins, filtes, and aggregations. '''

# Standard library imports
import sqlite3
import pathlib
import logging
import pandas as pd


###############################
#Logging
###############################

# Configure logging to write to a file, appending new logs to the existing file
logging.basicConfig(filename='log.txt', level=logging.DEBUG, filemode='a', format='%(asctime)s - %(levelname)s - %(message)s')

###############################
# File Paths
###############################

# Database file path
db_file_path = pathlib.Path('music_database.db')

# Define output folder path
output_folder_path = pathlib.Path('output')

#Define output file paths within the output folder
aggregation_output_file = output_folder_path / 'aggregation_results.txt'
filter_output_file = output_folder_path / 'filtered_results.txt'
group_by_output_file = output_folder_path / 'group_by_results.txt'
join_output_file = output_folder_path / 'join_results.txt'
sorting_output_file = output_folder_path / 'sorting_results.txt'

#SQL file path
insert_new_records_sql_path = pathlib.Path('sql') / 'insert_new_records.sql'
delete_records_sql_path = pathlib.Path('sql') / 'delete_records.sql'
query_aggregation_sql_path = pathlib.Path('sql') / 'query_aggregation.sql'
query_filter_sql_path = pathlib.Path('sql') / 'query_filter.sql'
query_group_by_sql_path = pathlib.Path('sql') / 'query_group_by.sql'
query_join_sql_path = pathlib.Path('sql') / 'query_join.sql'
query_sorting_sql_path = pathlib.Path('sql') / 'query_sorting.sql'
update_records_sql_path = pathlib.Path('sql') / 'update_records.sql'


###############################
# Define Functions
###############################

def insert_new_records(db_file_path):
    """Insert new records into the database."""
    try:
        with sqlite3.connect(db_file_path) as conn:
            with open(insert_new_records_sql_path, 'r') as file:
                sql_script = file.read()
            conn.executescript(sql_script)
            logging.info(f"Inserted new records from {insert_new_records_sql_path}")
    except sqlite3.Error as e:
        logging.exception(f"Error inserting new records: {e}")


#  This function is to help you confirm that the data has been successfully inserted into the artists and songs tables. It acts as a quick check to ensure that the database operations (like inserts) have worked as intended.
def verify_records(db_file_path):
    """Verify records in the tables."""
    try:
        with sqlite3.connect(db_file_path) as conn:
            artists_df = pd.read_sql_query("SELECT * FROM artists", conn)
            songs_df = pd.read_sql_query("SELECT * FROM songs", conn)
            print("Artists DataFrame:\n", artists_df)
            print("Songs DataFrame:\n", songs_df)
    except sqlite3.Error as e:
        print(f"Error verifying records: {e}")


def delete_records(db_file_path):
    """Delete records from the database."""
    try:
        with sqlite3.connect(db_file_path) as conn:
            with open(delete_records_sql_path, 'r') as file:
                sql_script = file.read()
            print(f"Executing DELETE SQL:\n{sql_script}")  # Log the SQL being executed
            conn.executescript(sql_script)
            logging.info(f"Deleted records using {delete_records_sql_path}")
    except sqlite3.Error as e:
        logging.exception(f"Error deleting records: {e}")

 #This query will produce a result set that indicates how many songs are associated with each artist.
def query_aggregation(db_file_path, output_file_path):
    """Perform aggregation queries and write results to a file."""
    try:
        with sqlite3.connect(db_file_path) as conn:
            with open(query_aggregation_sql_path, 'r') as file:
                sql_script = file.read()

            cursor = conn.cursor()
            cursor.execute(sql_script)
            
            results = cursor.fetchall()
            write_results_to_file(results, output_file_path, "Aggregation Query Results")
    except sqlite3.Error as e:
        logging.exception(f"Error executing aggregation queries: {e}")

#This filters data by a specified data element (like filtering all of the pop songs).
def query_filter(db_file_path, output_file_path):
    """Perform filtered queries."""
    try:
        with sqlite3.connect(db_file_path) as conn:
            with open(query_filter_sql_path, 'r') as file:
                sql_script = file.read()
        
            cursor = conn.cursor()
            cursor.execute(sql_script)
            
            # Fetch all results
            results = cursor.fetchall()
            
            # Write results to a file
            write_results_to_file(results, output_file_path, "Filtered Query Results")
    except sqlite3.Error as e:
        logging.exception(f"Error executing filtered queries: {e}")

#This will query the database and group the data (like showing how many songs each artist has in the table with one row for each artist).
def query_group_by(db_file_path, output_file_path):
    """Perform queries with GROUP BY clause."""
    try:
        with sqlite3.connect(db_file_path) as conn:
            with open(query_group_by_sql_path, 'r') as file:
                sql_script = file.read()
        
            cursor = conn.cursor()
            cursor.execute(sql_script)
            
            results = cursor.fetchall()
            write_results_to_file(results, output_file_path, "GROUP BY Query Results")
    except sqlite3.Error as e:

            logging.info(f"Executed GROUP BY queries from {query_group_by_sql_path}")
    except sqlite3.Error as e:
        logging.exception(f"Error executing GROUP BY queries: {e}")


#This will join columns from tables together (like joining the artist with the song name).
def query_join(db_file_path, output_file_path):
    """Perform queries with JOIN operations."""
    try:
        with sqlite3.connect(db_file_path) as conn:
            with open(query_join_sql_path, 'r') as file:
                sql_script = file.read()

            cursor = conn.cursor()
            cursor.execute(sql_script)
            
            results = cursor.fetchall()
            write_results_to_file(results, output_file_path, "JOIN Query Results")
    except sqlite3.Error as e:
        logging.exception(f"Error executing JOIN queries: {e}")

#This will query the database and sort specified data (like sorting songs by publiscation date).
def query_sorting(db_file_path, output_file_path):
    """Perform sorting queries."""
    try:
        with sqlite3.connect(db_file_path) as conn:
            with open(query_sorting_sql_path, 'r') as file:
                sql_script = file.read()

            cursor = conn.cursor()
            cursor.execute(sql_script)
            
            results = cursor.fetchall()
            write_results_to_file(results, output_file_path, "Sorting Query Results")
    except sqlite3.Error as e:
        logging.exception(f"Error executing sorting queries: {e}")

#This will update a record already in the database.
def update_records(db_file_path):
    """Update records in the database."""
    try:
        with sqlite3.connect(db_file_path) as conn:
            with open(update_records_sql_path, 'r') as file:
                sql_script = file.read()
            conn.executescript(sql_script)
            print(f"Executing UPDATE SQL:\n{sql_script}")  # Log the SQL being executed
            logging.info(f"Updated records using {update_records_sql_path}")
    except sqlite3.Error as e:
        logging.exception(f"Error updating records: {e}")

#This will write the results for a function to a specified file.
def write_results_to_file(results, output_file_path, title):
    """Write query results to a file with a title."""
    try:
         # Ensure the output folder exists
        output_file_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file_path, 'w') as file:
            file.write(f"{title}\n")
            for row in results:
                file.write(f"{row}\n")
        logging.info(f"Wrote results to {output_file_path}")
    except IOError as e:
        logging.exception(f"Error writing results to file: {e}")



#####################################
#Define Main Function to call functions
#####################################

def main():
    logging.info("Program started")
    insert_new_records(db_file_path)
    verify_records(db_file_path)
    delete_records(db_file_path)
    update_records(db_file_path)

    # Specify the output file paths for results
    query_aggregation(db_file_path, aggregation_output_file)  # Write aggregation results to file
    query_filter(db_file_path, filter_output_file)  # Write filtered results to file
    query_group_by(db_file_path, group_by_output_file)  # Write group by results to file
    query_join(db_file_path, join_output_file)  # Write join results to file
    query_sorting(db_file_path, sorting_output_file)  # Write sorting results to file

    logging.info("Program ended")

#####################################
# Conditional Execution
#####################################

# Conditionally execute the main() function if this is the script being run
if __name__ == "__main__":
    main()