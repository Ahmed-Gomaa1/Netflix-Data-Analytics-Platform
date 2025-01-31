import unicodedata
import pandas as pd 
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, ForeignKey, select, text,Enum,Text,Date
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
import sys
sys.stdout.reconfigure(encoding='utf-8')

DATABASE_URL = "mssql+pyodbc://3hmed@./NetflixShows?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()
metadata = MetaData()

Show = Table(
    'Show', metadata,
    Column('show_id', Integer, primary_key=True),
    Column('title', String(255), nullable=False),
    Column('type', Enum('Movie', 'TV Show', name='show_type'), nullable=False),
    Column('release_year', Integer, nullable=False),
    Column('description', Text, nullable=True)
)
Director = Table(
    'Director', metadata,
    Column('director_id', Integer, primary_key=True, autoincrement=True),
    Column('name', String(255), unique=True, nullable=False)
)
Cast = Table(
    'Cast', metadata,
    Column('cast_id', Integer, primary_key=True, autoincrement=True),
    Column('name', String(255), unique=True, nullable=False)
)
Country = Table(
    'Country', metadata,
    Column('country_id', Integer, primary_key=True, autoincrement=True),
    Column('name', String(255), unique=True, nullable=False)
)
Genre = Table(
    'Genre', metadata,
    Column('genre_id', Integer, primary_key=True, autoincrement=True),
    Column('name', String(255), unique=True, nullable=False)
)
Show_Metadata = Table(
    'Show_Metadata', metadata,
    Column('metadata_id', Integer, primary_key=True, autoincrement=True),
    Column('show_id', Integer, ForeignKey('Show.show_id'), nullable=False),
    Column('date_added', Date, nullable=True),
    Column('rating', String(10), nullable=True),
    Column('duration', String(50), nullable=True)
)
Show_Director = Table(
    'Show_Director', metadata,
    Column('show_id', Integer, ForeignKey('Show.show_id'), primary_key=True),
    Column('director_id', Integer, ForeignKey('Director.director_id'), primary_key=True)
)
Show_Cast = Table(
    'Show_Cast', metadata,
    Column('show_id', Integer, ForeignKey('Show.show_id'), primary_key=True),
    Column('cast_id', Integer, ForeignKey('Cast.cast_id'), primary_key=True)
)
Show_Country = Table(
    'Show_Country', metadata,
    Column('show_id', Integer, ForeignKey('Show.show_id'), primary_key=True),
    Column('country_id', Integer, ForeignKey('Country.country_id'), primary_key=True)
)
Show_Genre = Table(
    'Show_Genre', metadata,
    Column('show_id', Integer, ForeignKey('Show.show_id'), primary_key=True),
    Column('genre_id', Integer, ForeignKey('Genre.genre_id'), primary_key=True)
)
metadata.create_all(engine)
print("Tables created successfully.")

def normalize_name(name):
    name = name.lower().strip()
    name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('ascii')
    return name

def insert_data_from_csv(csv_path):
    data = pd.read_csv(csv_path).fillna(' ')

    for _, row in data.iterrows():
        try:
            insert_stmt = Show.insert().values(
                show_id=row['show_id'], 
                title=row['title'],
                type=row['type'],
                release_year=row['release_year'],
                description=row['description']
            )
            session.execute(insert_stmt)
            # Handle Directors
            for director in row['director'].split(','):
                director = director.strip()
                if director:
                    normalized_director = normalize_name(director)

                    check_stmt = text("""
                        SELECT COUNT(1) 
                        FROM Director 
                        WHERE name COLLATE Latin1_General_CI_AI = :director
                    """)
                    exists = session.execute(check_stmt, {'director': normalized_director}).scalar()

                    if not exists:
                        insert_stmt = text("INSERT INTO Director (name) VALUES (:director)")
                        session.execute(insert_stmt, {'director': director})

                    director_id_query = select(Director.c.director_id).where(Director.c.name == director)
                    director_id = session.execute(director_id_query).scalar()

                    session.execute(Show_Director.insert().values(show_id=row['show_id'], director_id=director_id))

            # Handle Cast
            for cast_member in row['cast'].split(','):
                cast_member = cast_member.strip()
                if cast_member:
                    normalized_cast_member = normalize_name(cast_member)

                    check_stmt = text("""
                        SELECT COUNT(1) 
                        FROM Cast 
                        WHERE name COLLATE Latin1_General_CI_AI = :cast_member
                    """)
                    exists = session.execute(check_stmt, {'cast_member': normalized_cast_member}).scalar()

                    if not exists:
                        insert_stmt = text("INSERT INTO Cast (name) VALUES (:cast_member)")
                        session.execute(insert_stmt, {'cast_member': cast_member})

                    cast_id_query = select(Cast.c.cast_id).where(Cast.c.name == cast_member)
                    cast_id = session.execute(cast_id_query).scalar()

                    session.execute(Show_Cast.insert().values(show_id=row['show_id'], cast_id=cast_id))

            # Handle Countries
            for country in row['country'].split(','):
                country = country.strip()
                if country:
                    normalized_country = normalize_name(country)

                    check_stmt = text("""
                        SELECT COUNT(1) 
                        FROM Country 
                        WHERE name COLLATE Latin1_General_CI_AI = :country
                    """)
                    exists = session.execute(check_stmt, {'country': normalized_country}).scalar()

                    if not exists:
                        insert_stmt = text("INSERT INTO Country (name) VALUES (:country)")
                        session.execute(insert_stmt, {'country': country})

                    country_id_query = select(Country.c.country_id).where(Country.c.name == country)
                    country_id = session.execute(country_id_query).scalar()

                    session.execute(Show_Country.insert().values(show_id=row['show_id'], country_id=country_id))

            # Handle Genres
            for genre in row['listed_in'].split(','):
                genre = genre.strip()
                if genre:
                    normalized_genre = normalize_name(genre)

                    check_stmt = text("""
                        SELECT COUNT(1) 
                        FROM Genre 
                        WHERE name COLLATE Latin1_General_CI_AI = :genre
                    """)
                    exists = session.execute(check_stmt, {'genre': normalized_genre}).scalar()

                    if not exists:
                        insert_stmt = text("INSERT INTO Genre (name) VALUES (:genre)")
                        session.execute(insert_stmt, {'genre': genre})

                    genre_id_query = select(Genre.c.genre_id).where(Genre.c.name == genre)
                    genre_id = session.execute(genre_id_query).scalar()

                    session.execute(Show_Genre.insert().values(show_id=row['show_id'], genre_id=genre_id))

            # Insert Metadata
            metadata_stmt = Show_Metadata.insert().values(
                show_id=row['show_id'],
                date_added=row['date_added'],
                rating=row['rating'],
                duration=row['duration']
            )
            session.execute(metadata_stmt)
            session.commit()

        except SQLAlchemyError as e:
            print(f"Error processing row: {row}. Error: {e}")
            session.rollback()
    print("Data inserted successfully!")

csv_path = 'Day2 Design and Implement DB and interacting with Python Libraries/netflix_titles.csv'
insert_data_from_csv(csv_path)
