import psycopg2
from random import randint, choice
import string
import time


def random_generator(size, chars=string.ascii_uppercase + string.digits):
    return ''.join(choice(chars) for x in range(size))


def create_benchmark_table(conn):
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE benchmark (
        theKey integer PRIMARY KEY,
        columnA integer,
        columnB integer,
        fillter text
    )
    """)


def drop_benchmark_table(conn):
    try:
        cur = conn.cursor()
        cur.execute("""
            DROP TABLE benchmark
            """)
    except:
        print("Failed to drop table! Proceeding...")


def print_benchmark_table(conn):
    cur = conn.cursor()
    cur.execute('SELECT * FROM benchmark')
    all = cur.fetchall()
    print(all)


def part1(conn):
    start = time.time()
    for i in range(0, 4999999):
        cur = conn.cursor()
        cur.execute("INSERT INTO Benchmark VALUES (%s, %s, %s, %s)",
                    (i, randint(1, 50000), randint(1, 50000), random_generator(randint(1, 247))))
        conn.commit()
    end = time.time()
    return (str(end - start) + " seconds")

def part2(conn):
    start = time.time()
    indexes = []
    for i in range(0, 4999999):
        indexes.append(i)

    while len(indexes) != 0:
        cur = conn.cursor()
        randomint = indexes[randint(0, len(indexes) - 1)]
        cur.execute("INSERT INTO Benchmark VALUES (%s, %s, %s, %s)",
                    (randomint, randint(1, 50000), randint(1, 50000), random_generator(randint(1, 247))))
        conn.commit()
        indexes.remove(randomint)
    end = time.time()
    return (str(end - start) + " seconds")


conn = psycopg2.connect(host="localhost",database="hw6", user="postgres", password="postgres")
drop_benchmark_table(conn)
create_benchmark_table(conn)

# part 1
time1 = part1(conn)
print("Finished part 1")

print_benchmark_table(conn)
drop_benchmark_table(conn)
create_benchmark_table(conn)

# part 2
time2 = part2(conn)

print_benchmark_table(conn)
drop_benchmark_table(conn)

print("TOTAL TIMES:")
print("******************************")
print("In order PK:     " + time1)
print("Random order PK: " + time2)
print("******************************")