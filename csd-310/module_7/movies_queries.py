# Ian Spresney
# CSD 310
# 11/24/2023
# Module 7.2 Assignment
# Sources: https://www.geeksforgeeks.org/sql-using-python/

import mysql.connector


# Function to connect to the MySQL database
def connect_to_database():
    config = {
        "user": "movies_user",
        "password": "popcorn",
        "host": "127.0.0.1",
        "database": "movies",
        "raise_on_warnings": True
    }

    try:
        db = mysql.connector.connect(**config)
        print("\nDatabase user {} connected to MySQL on host {} with database {}".format(config["user"], config["host"], config["database"]))
        return db
    except mysql.connector.Error as err:
        print("Error: {}".format(err))
        return None

# Function to execute a query and print the results
def execute_query(cursor, query, description):
    print(description)
    cursor.execute(query)
    results = cursor.fetchall()
    for result in results:
        print(result)
    print("\n")

def main():
    # Connect to the MySQL database
    db = connect_to_database()

    if db is not None:
        try:
            # Create a cursor
            cursor = db.cursor()

            # Query 1: Select all fields from the studio table
            query1 = "SELECT * FROM studio;"
            execute_query(cursor, query1, "Query 1: Select all fields from the studio table")

            # Query 2: Select all fields from the genre table
            query2 = "SELECT * FROM genre;"
            execute_query(cursor, query2, "Query 2: Select all fields from the genre table")

            # Query 3: Select movie names for movies with a run time of less than two hours
            query3 = "SELECT movie_name FROM movies WHERE run_time < 120;"
            execute_query(cursor, query3, "Query 3: Movie names for movies with a run time of less than two hours")

            # Query 4: Get a list of film names and directors ordered by director
            query4 = "SELECT film_name, director FROM movies ORDER BY director;"
            execute_query(cursor, query4, "Query 4: List of film names and directors ordered by director")

        except mysql.connector.Error as err:
            print("Error: {}".format(err))

        finally:
            # Close the cursor and connection
            cursor.close()
            db.close()
            print("\nDatabase connection closed.")

if __name__ == "__main__":
    main()
