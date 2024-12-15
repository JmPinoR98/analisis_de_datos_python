import socket

DATABASE_CONFIG = {
    "transact": {
        "host": socket.gethostbyname(socket.gethostname()),
        "port": 3310,
        "user": "root",
        "password": "root",
        "database": "db_movies_netflix_transact"
    },
    "warehouse": {
        "host": socket.gethostbyname(socket.gethostname()),
        "port": 3310,
        "user": "root",
        "password": "root",
        "database": "dw_netflix"
    }
}

CSV_FILES = {
    'users': r'./data/users.csv',
    'award_participants': r'./data/Awards_participant.csv',
    'award_movie': r'./data/Awards_movie.csv'
}

LOG_FILE = 'logs/pipeline.log'

QUERY = """
    SELECT 
        movie.movieID as movieID, 
        movie.movieTitle as title, 
        movie.releaseDate as releaseDate, 
        gender.name as gender, 
        person.name as participantName, 
        participant.participantRole as roleparticipant 
    FROM movie 
    INNER JOIN participant 
    ON movie.movieID=participant.movieID
    INNER JOIN person
    ON person.personID = participant.personID
    INNER JOIN movie_gender 
    ON movie.movieID = movie_gender.movieID
    INNER JOIN gender 
    ON movie_gender.genderID = gender.genderID
"""