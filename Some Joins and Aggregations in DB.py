import pandas as pd
from sqlalchemy import create_engine, text

# Set up the database connection
DATABASE_URL = "mssql+pyodbc://3hmed@./NetflixShows?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(DATABASE_URL)

# Function to execute SQL queries and return results as pandas DataFrame
def execute_query(query):
    with engine.connect() as connection:
        result = connection.execute(text(query))
        # Convert the result to a pandas DataFrame
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df

# 1. Count the Number of Shows Per Genre
query_1 = """
SELECT TOP 10
    g.name AS genre,
    COUNT(s.show_id) AS show_count
FROM 
    Show_Genre sg
JOIN 
    Genre g ON sg.genre_id = g.genre_id
JOIN 
    Show s ON sg.show_id = s.show_id
GROUP BY 
    g.name;
"""
result_1 = execute_query(query_1)
print("Count of Shows Per Genre:")
print(result_1)

# 2. List of Shows and Directors
query_2 = """
SELECT TOP 10
    s.title AS show_title,
    d.name AS director_name
FROM 
    Show_Director sd
JOIN 
    Show s ON sd.show_id = s.show_id
JOIN 
    Director d ON sd.director_id = d.director_id;
"""
result_2 = execute_query(query_2)
print("\nShows and Directors:")
print(result_2)

# 3. Count of Cast Members in Each Show
query_3 = """
SELECT TOP 10
    s.title AS show_title,
    COUNT(c.cast_id) AS cast_count
FROM 
    Show_Cast sc
JOIN 
    Show s ON sc.show_id = s.show_id
JOIN 
    Cast c ON sc.cast_id = c.cast_id
GROUP BY 
    s.title;
"""
result_3 = execute_query(query_3)
print("\nCast Count Per Show:")
print(result_3)

# 4. Total Number of Shows Per Country
query_4 = """
SELECT TOP 10
    c.name AS country,
    COUNT(s.show_id) AS total_shows
FROM 
    Show_Country sc
JOIN 
    Country c ON sc.country_id = c.country_id
JOIN 
    Show s ON sc.show_id = s.show_id
GROUP BY 
    c.name;
"""
result_4 = execute_query(query_4)
print("\nTotal Shows Per Country:")
print(result_4)

# 5. Genre Distribution for Each Show Type (Movie/TV Show)
query_5 = """
SELECT TOP 10
    s.type AS show_type,
    g.name AS genre,
    COUNT(sg.show_id) AS show_count
FROM 
    Show_Genre sg
JOIN 
    Genre g ON sg.genre_id = g.genre_id
JOIN 
    Show s ON sg.show_id = s.show_id
GROUP BY 
    s.type, g.name;
"""
result_5 = execute_query(query_5)
print("\nGenre Distribution for Each Show Type:")
print(result_5)

# 6. Most Frequent Cast Members (Top 10)
query_6 = """
SELECT TOP 10
    c.name AS cast_member,
    COUNT(sc.show_id) AS show_count
FROM 
    Show_Cast sc
JOIN 
    Cast c ON sc.cast_id = c.cast_id
GROUP BY 
    c.name
ORDER BY 
    COUNT(sc.show_id) DESC;
"""
result_6 = execute_query(query_6)
print("\nTop 10 Most Frequent Cast Members:")
print(result_6)
