import psycopg2
from random import randint, choice, shuffle
import string
import threading
import time


def random_generator(size, chars=string.ascii_uppercase + string.digits):
    return ''.join(choice(chars) for x in range(size))

amount_of_rows = 5000000
number_of_threads = 4
COLUMN_A = []
COLUMN_B = []
COLUMN_C = []
Q2INDEXES = []

def populate_arrays(threadNumber, totalThreads):
    for i in range(threadNumber * (int(amount_of_rows / totalThreads)),
                   (threadNumber + 1) * (int(amount_of_rows / totalThreads))):
        COLUMN_A.append(randint(1, 50000))
        COLUMN_B.append(randint(1, 50000))
        COLUMN_C.append(random_generator(randint(1, 247)))
        Q2INDEXES.append(i)

threads = []
for i in range(0, number_of_threads):
    thread = threading.Thread(target=populate_arrays, args=(i, number_of_threads))
    threads.append(thread)

for thread in threads:
    thread.start()

for thread in threads:
    thread.join()

shuffle(Q2INDEXES)
print("Finished Generating Arrays!")

def create_benchmark_table(conn):
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE benchmark (
        theKey integer PRIMARY KEY,
        columnA integer,
        columnB integer,
        filter text
    )
    """)
    conn.commit()

def countRows(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM benchmark;
        """)
    conn.commit()
    all = cur.fetchall()
    print(all)



def createSecondaryIndex2(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE INDEX a ON benchmark(columnA);
        """)
    conn.commit()

def createSecondaryIndex3(conn):
    cur = conn.cursor()
    cur.execute("""
            CREATE INDEX b ON benchmark(columnB);
            """)
    conn.commit()

def createSecondaryIndex4(conn):
    cur = conn.cursor()
    cur.execute("""
            CREATE INDEX a ON benchmark(columnA);
            """)
    conn.commit()
    cur = conn.cursor()
    cur.execute("""
                CREATE INDEX ab ON benchmark(columnB);
                """)
    conn.commit()

def drop_benchmark_table(conn):
    try:
        cur = conn.cursor()
        cur.execute("""
            DROP TABLE benchmark
            """)
        conn.commit()
    except Exception:
        print("Failed to drop table! Proceeding...")
        conn.close()
        conn = psycopg2.connect(host="localhost", database="hw6", user="postgres", password="postgres")
    # finally:
    #     conn.close()


def print_benchmark_table(conn):
    cur = conn.cursor()
    cur.execute('SELECT * FROM benchmark')
    all = cur.fetchall()
    print(all)


def part1(conn, threadNumber, totalThreads=10):
    tempConn = psycopg2.connect(host="localhost", database="hw6", user="postgres", password="postgres")
    cur = tempConn.cursor()
    for i in range(threadNumber * (int(amount_of_rows / totalThreads)),
                   (threadNumber + 1) * (int(amount_of_rows / totalThreads))):
        cur.execute("INSERT INTO Benchmark VALUES (%s, %s, %s, %s)",
                    (i, COLUMN_A[i], COLUMN_B[i], COLUMN_C[i]))
        tempConn.commit()



def part2(conn, threadNumber, totalThreads):
    tempConn = psycopg2.connect(host="localhost", database="hw6", user="postgres", password="postgres")
    cur = tempConn.cursor()
    for i in range(threadNumber * (int(amount_of_rows / totalThreads)),
                   (threadNumber + 1) * (int(amount_of_rows / totalThreads))):
        cur.execute("INSERT INTO Benchmark VALUES (%s, %s, %s, %s)",
                    (Q2INDEXES[i], COLUMN_A[i], COLUMN_B[i], COLUMN_C[i]))
        tempConn.commit()


def load1(conn):
    threads = []
    for i in range(0, number_of_threads):
        thread = threading.Thread(target=part1, args=(conn, i, number_of_threads))
        threads.append(thread)

    start = time.time()
    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    end = time.time()
    # countRows(conn)
    return (str(end - start) + " seconds")


def load2(conn):
    threads = []
    for i in range(0, number_of_threads):
        thread = threading.Thread(target=part2, args=(conn, i, number_of_threads))
        threads.append(thread)

    start = time.time()
    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    end = time.time()
    # countRows(conn)
    return (str(end - start) + " seconds")

def query1(conn, constant):
    start = time.time()
    cur = conn.cursor()
    cur.execute('SELECT * FROM benchmark'
                ' WHERE benchmark.columnA = ' + str(constant))
    end = time.time()
    return (end - start)

def query2(conn, constant):
    start = time.time()
    cur = conn.cursor()
    cur.execute('SELECT * FROM benchmark'
                ' WHERE benchmark.columnB = ' + str(constant))
    end = time.time()
    return (end - start)

def query3(conn, constant):
    start = time.time()
    cur = conn.cursor()
    cur.execute('SELECT * FROM benchmark'
                ' WHERE benchmark.columnA = ' + str(constant) +
                ' AND benchmark.columnB = ' + str(constant))
    end = time.time()
    return (end - start)


def part1Main(conn):
    # part 1
    time1 = load1(conn)
    drop_benchmark_table(conn)
    create_benchmark_table(conn)
    print("Finished part 1")

    # part 2
    time2 = load2(conn)

    print("TOTAL TIMES:")
    print("******************************")
    print("In order PK:     " + time1)
    print("Random order PK: " + time2)
    print("******************************")

def queryResults(conn):
    query1List = []
    query2List = []
    query3List = []

    randomInts = [5312, 6321, 26810, 10, 42190, 19672, 54721, 12390, 81706, 18103]
    for i in range(0, 9):
        query1List.append(query1(conn, randomInts[i]))

    for i in range(0, 9):
        query2List.append(query2(conn, randomInts[i]))

    for i in range(0, 9):
        query3List.append(query3(conn, randomInts[i]))

    return (sum(query1List) / len(query1List), 
            sum(query2List) / len(query2List), 
            sum(query3List) / len(query3List))


def part2Main(conn):
    results = []

    drop_benchmark_table(conn)
    create_benchmark_table(conn)
    load_time1 = load1(conn)
    query1Time, query2Time, query3Time = queryResults(conn)
    drop_benchmark_table(conn)
    create_benchmark_table(conn)
    load_time2 = load2(conn)
    query1Time2, query2Time2, query3Time2 = queryResults(conn)
    results.append([load_time1, load_time2, query1Time, query1Time2, query2Time, query2Time2, query3Time, query3Time2])

    drop_benchmark_table(conn)
    create_benchmark_table(conn)
    createSecondaryIndex2(conn)
    load_time1 = load1(conn)
    query1Time, query2Time, query3Time = queryResults(conn)
    drop_benchmark_table(conn)
    create_benchmark_table(conn)
    createSecondaryIndex2(conn)
    load_time2 = load2(conn)
    query1Time2, query2Time2, query3Time2 = queryResults(conn)
    results.append([load_time1, load_time2, query1Time, query1Time2, query2Time, query2Time2, query3Time, query3Time2])

    drop_benchmark_table(conn)
    create_benchmark_table(conn)
    createSecondaryIndex3(conn)
    load_time1 = load1(conn)
    query1Time, query2Time, query3Time = queryResults(conn)
    drop_benchmark_table(conn)
    create_benchmark_table(conn)
    createSecondaryIndex3(conn)
    load_time2 = load2(conn)
    query1Time2, query2Time2, query3Time2 = queryResults(conn)
    results.append([load_time1, load_time2, query1Time, query1Time2, query2Time, query2Time2, query3Time, query3Time2])

    drop_benchmark_table(conn)
    create_benchmark_table(conn)
    createSecondaryIndex4(conn)
    load_time1 = load1(conn)
    query1Time, query2Time, query3Time = queryResults(conn)
    drop_benchmark_table(conn)
    create_benchmark_table(conn)
    createSecondaryIndex4(conn)
    load_time2 = load2(conn)
    query1Time2, query2Time2, query3Time2 = queryResults(conn)
    results.append([load_time1, load_time2, query1Time, query1Time2, query2Time, query2Time2, query3Time, query3Time2])

    print("RESULTS:")
    for row in results:
        print(row)


def sideIssue(conn):
    start = time.time()
    drop_benchmark_table(conn)
    create_benchmark_table(conn)
    load1(conn)
    createSecondaryIndex4(conn)
    end = time.time()
    time1 = str(end - start) + " seconds"

    start = time.time()
    drop_benchmark_table(conn)
    create_benchmark_table(conn)
    load2(conn)
    createSecondaryIndex4(conn)
    end = time.time()
    time2 = str(end - start) + " seconds"

    print("****************Side Issue*****************")
    print(time1)
    print(time2)
    print("*******************************************")

conn = psycopg2.connect(host="localhost",database="hw6", user="postgres", password="postgres")
# drop_benchmark_table(conn)
create_benchmark_table(conn)

part1Main(conn)
part2Main(conn)
sideIssue(conn)

drop_benchmark_table(conn)