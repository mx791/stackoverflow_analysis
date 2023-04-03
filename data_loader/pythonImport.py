import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timezone
import psycopg2
import time
import random
import os

def preprocess_string(text):
    text = text.lower()
    text = text.replace("\n\r", ".")
    text = text.replace("\n", ".")
    text = text.replace("\r", ".")
    text = text.replace("...", ".")
    text = text.replace("..", ".")
    text = text.replace("  ", " ")
    return text

def get_questions_meta(URL):

    content = requests.get(URL)
    doc = BeautifulSoup(content.content, 'html.parser')
    articles = doc.find_all("div", attrs={'class': 's-post-summary'})
    data = []
    for article in articles:
        try:
            title_element = article.find("h3", attrs={"class": "s-post-summary--content-title"})
            title = title_element.find("a").text

            description = article.find("div", attrs={"class": "s-post-summary--content-excerpt"}).string

            link = article.find("a")["href"]
            uid = link.split("/")[2]

            categories = article.find("ul").find_all("li")
            categories = [categorie.text for categorie in categories]

            time = re.findall("[0-9-: ]+", article.find("time").find("span")["title"])[0]
            time = datetime.strptime(time, '%Y-%m-%d %H:%M:%S')

            data.append({
                "title": title,
                "description": preprocess_string(description),
                "categories": categories,
                "time": time,
                "link": link,
                "uid": uid
            })
        except:
            pass
        
    return data


def get_url(start, end):
    while True:
        i = random.randint(start, end-1)
        if i <= 1:
            yield "https://stackoverflow.com/questions?tab=votes"
        else:
            yield "https://stackoverflow.com/questions?tab=votes&pagesize=50&page=" + str(i)
        

def create_client():
    return psycopg2.connect(
        database=os.environ.get("DB_NAME", "main"),
        host=os.environ.get("HOST_URL", "localhost"),
        user=os.environ.get("DB_USER", "user"),
        password=os.environ.get("DB_PASS", "pass"),
        port=os.environ.get("DB_PORT", "5432")
    )

client = create_client()
client.set_session(autocommit=True)

def sql_wrapper_insert(sql: str, args) -> int:
    cursor = client.cursor()
    cursor.execute(sql, args)
    id = cursor.fetchone()[0]
    cursor.close()
    client.commit()
    return id

categories = {}
def get_category_id(cat: str) -> int:
    if cat in categories:
        return categories[cat]
    
    cursor = client.cursor()
    cursor.execute("SELECT * FROM category WHERE name = %s", ([cat]))
    result = cursor.fetchone()

    if result:
        categories[cat] = result[0]
        return result[0]
    
    return sql_wrapper_insert("INSERT INTO category (name) VALUES (%s) RETURNING id", ([cat]))


def insert_question(art):
    return sql_wrapper_insert("INSERT INTO question (uid, title, description, asked_at) VALUES (%s, %s, %s, %s) RETURNING id"
        , (art["uid"], art["title"], art["description"], art["time"]))

def link_question_category(qid, catid):
    return sql_wrapper_insert(
        "INSERT INTO question_category (question_id, category_id) VALUES (%s, %s) RETURNING id"
        , (qid, catid)
    )

def push_data(data):
    ok = 0
    for line in data:
        try:
            qid = insert_question(line)
            for cat in line["categories"]:
                cat_id = get_category_id(cat)
                link_question_category(qid, cat_id)
            ok += 1
        except: 
            pass
    print(ok, "/", len(data))

def drop_all():
    client = create_client()
    cursor = client.cursor()
    cursor.execute("DROP TABLE question")
    cursor.execute("DROP TABLE category")
    cursor.execute("DROP TABLE question_category")
    cursor.close()
    client.commit()

def init_schema():

    client = create_client()
    cursor = client.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS question (
        id SERIAL,
        uid INT UNIQUE,
        title VARCHAR(512),
        description VARCHAR(512),
        asked_at TIMESTAMP
    );""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS category (
        id SERIAL,
        name VARCHAR(512)
    );""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS question_category (
        id SERIAL,
        question_id INT,
        category_id INT
    );""")

    cursor.close()
    client.commit()

def count():
    cursor = client.cursor()
    cursor.execute("select count(*) from question")
    return cursor.fetchone()

def main_loop():

    counter = 0
    for url in get_url(counter, 407000):
        arts = get_questions_meta(url)
        push_data(arts)
        print("indexing", url)
        print("question count=", count())
        if len(arts) == 0:
            time.sleep(60)

if __name__ == "__main__":
    print("Starting all...")
    if os.environ.get("SHOULD_RESET", False):
        print("reseting tables")
        drop_all()
    print("Updating schema")
    init_schema()
    print("Schema updated")
    main_loop()
