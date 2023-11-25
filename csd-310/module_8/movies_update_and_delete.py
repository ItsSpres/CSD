# Ian Spresney
# CSD310
# Module 8.2
# 11/25/23
# Sources: 
# https://dev.mysql.com/doc/connector-python/en/
# https://www.mysqltutorial.org/python-mysql/
# https://dev.mysql.com/doc/connector-python/en/connector-python-api-errors.html
# https://dongr0510.medium.com/how-to-use-python-cursors-fetchall-fetchmany-fetchone-to-read-records-from-sql-34a0ff456abf






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

def close_database_connection(db):
    if db:
        db.close()
        print("Database connection closed.")

def get_genre_id(cursor, genre_name):
    query = "SELECT genre_id FROM genre WHERE genre_name = %s"
    cursor.execute(query, (genre_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    return None

def get_studio_id(cursor, studio_name):
    query = "SELECT studio_id FROM studio WHERE studio_name = %s"
    cursor.execute(query, (studio_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    return None

def show_films(cursor, title):
    # Inner join query
    query = """
        SELECT film_name, film_director, genre_name, studio_name
        FROM film
        INNER JOIN genre ON film.genre_id = genre.genre_id
        INNER JOIN studio ON film.studio_id = studio.studio_id
    """
    
    # Execute the query
    cursor.execute(query)

    # Fetch all results
    films = cursor.fetchall()

    # Format and display the results
    print(f"{title} --")
    for film in films:
        print(f"Film Name: {film[0]}\nDirector: {film[1]}\nGenre Name ID: {film[2]}\nStudio Name: {film[3]}\n")

# Example usage:
db = connect_to_database()
if not db:
    # Handle the case where the database connection failed
    exit()

try:
    cursor = db.cursor()

    # Example usage:
    show_films(cursor, "DISPLAYING FILMS")
     
    # Insert a new record
    try:
        genre_id = get_genre_id(cursor, 'Horror')
        studio_id = get_studio_id(cursor, 'Universal Pictures')

        insert_query = """
            INSERT INTO film (film_name, film_director, genre_id, studio_id, film_releaseDate, film_runtime)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, ('Indiana Jones', 'George Lucas', genre_id, studio_id, 2023, 160))

        db.commit()

        # Display films after insert
        show_films(cursor, "DISPLAYING FILMS AFTER INSERT")

    except mysql.connector.Error as err:
        print("Error during insertion: {}".format(err))
        db.rollback()  # Rollback the transaction to avoid leaving incomplete data in the database

    # Update the film 'Alien' to be a Horror film
    try:
        horror_genre_id = get_genre_id(cursor, 'Horror')
        update_query = """
            UPDATE film
            SET genre_id = %s
            WHERE film_name = 'Alien'
        """
        cursor.execute(update_query, (horror_genre_id,))
        db.commit()

        # Display films after update
        show_films(cursor, "DISPLAYING FILMS AFTER UPDATE Changed Alien to Horror")

    except mysql.connector.Error as err:
        print("Error during update: {}".format(err))
        db.rollback()

    # Delete the movie 'Gladiator'
    try:
        delete_query = "DELETE FROM film WHERE film_name = 'Gladiator'"
        cursor.execute(delete_query)
        db.commit()

        # Display films after delete
        show_films(cursor, "DISPLAYING FILMS AFTER DELETE")

    except mysql.connector.Error as err:
        print("Error during delete: {}".format(err))
        db.rollback()

finally:
    # Close the connection in a finally block to ensure it happens even if an exception occurs
    close_database_connection(db)

