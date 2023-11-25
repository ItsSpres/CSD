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

# Function to execute a query and print the results with a custom name
def execute_query(cursor, query, custom_name, format_string):
    print(custom_name)
    cursor.execute(query)
    results = cursor.fetchall()
    for result in results:
        print(format_string.format(*result))
    print("\n")

def main():
    # Connect to the MySQL database
    db = connect_to_database()

    if db is not None:
        try:
            # Create a cursor
            cursor = db.cursor()

            # Query 1: Select all fields from the studio table
            custom_name1 = "-- DISPLAYING Studio RECORDS --"
            format_string1 = "Studio ID: {0}\nStudio Name: {1}\n"
            query1 = "SELECT * FROM studio;"
            execute_query(cursor, query1, custom_name1, format_string1)

            # Query 2: Select all fields from the genre table
            custom_name2 = "-- DISPLAYING Genre RECORDS --"
            format_string2 = "Genre ID: {0}\nGenre Name: {1}\n"
            query2 = "SELECT * FROM genre;"
            execute_query(cursor, query2, custom_name2, format_string2)

            # Query 3: Select movie names for movies with a run time of less than two hours
            custom_name3 = "-- DISPLAYING Short Film RECORDS --"
            format_string3 = "Film Name: {0}\nRuntime: {1}\n"
            query3 = "SELECT film_name, film_runtime FROM film WHERE film_runtime < 120;"
            execute_query(cursor, query3, custom_name3, format_string3)

            # Query 4: Get a list of film names and directors ordered by director
            custom_name4 = "-- DISPLAYING Director RECORDS in Order --"
            format_string4 = "Film Name: {0}\nDirector: {1}\n"
            query4 = "SELECT film_name, film_director FROM film ORDER BY film_director;"
            execute_query(cursor, query4, custom_name4, format_string4)

        except mysql.connector.Error as err:
            print("Error: {}".format(err))

        finally:
            # Close the cursor and connection
            cursor.close()
            db.close()
            print("\nDatabase connection closed.")

if __name__ == "__main__":
    main()
