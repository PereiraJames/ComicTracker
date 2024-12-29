import mysql.connector
from marvel import Marvel
from comickeys import PUBLIC_KEY, PRIVATE_KEY

# Define Marvel API client
marvel = Marvel(PUBLIC_KEY=PUBLIC_KEY, PRIVATE_KEY=PRIVATE_KEY)

# List of series IDs for Spider-Man
listofSeriesID = [
    "454",  # Amazing Spider-Man (1999-2013)
    "1987",  # The Amazing Spider-Man (1963-1998)
    "20432",  # The Amazing Spider-Man (2015-2018)
    "17285",  # The Amazing Spider-Man (2014-2015)
    "24396"  # The Amazing Spider-Man (2018-2022)
]

# Database connection details
db_config = {
    'host': "192.168.1.56",
    'user': "jason",
    'passwd': "jason",
    'database': "marvelcomics"
}

def insert_comic_data(cursor, seriesid, comic_data):
    """Inserts comic data into the database or updates if the issueId exists."""
    query = """
        INSERT INTO comicsDB (seriesid, issueId, comicTitle, comicIssue, comicImageURL)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            comicTitle = VALUES(comicTitle),
            comicIssue = VALUES(comicIssue),
            comicImageURL = VALUES(comicImageURL)
    """
    cursor.execute(query, (
        seriesid, comic_data['id'], comic_data['title'],
        comic_data['issueNumber'], f"{comic_data['thumbnail_path']}.{comic_data['thumbnail_extension']}"
    ))

def main():
    countOfComicsImported = 0
    db = mysql.connector.connect(**db_config)
    cursor = db.cursor()

    try:
        for seriesid in listofSeriesID:
            offsetValue = 0
            print(f"Importing SERIESID: {seriesid}")

            while True:
                print(f"Importing SERIESID: {seriesid} with Offset: {offsetValue}")
                search_results = marvel.series.comics(
                    seriesid, orderBy="title", limit=100, offset=offsetValue, noVariants=True
                )['data']['results']

                if not search_results:
                    print(f"Completed SERIESID: {seriesid} with Offset: {offsetValue}")
                    break

                offsetValue += 100

                for comic in search_results:
                    comic_data = {
                        'id': comic['id'],
                        'issueNumber': comic['issueNumber'],
                        'title': comic['title'],
                        'thumbnail_path': comic['thumbnail']['path'],
                        'thumbnail_extension': comic['thumbnail']['extension']
                    }

                    insert_comic_data(cursor, seriesid, comic_data)
                    countOfComicsImported += 1

                # Commit after each batch of comics
                db.commit()

        print(f"Total Comics Imported: {countOfComicsImported}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        db.close()

if __name__ == "__main__":
    main()
