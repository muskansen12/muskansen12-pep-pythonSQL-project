import csv
import sqlite3

# Connect to the SQLite in-memory database
conn = sqlite3.connect(':memory:')

# A cursor object to execute SQL commands
cursor = conn.cursor()


def main():

    # users table
    cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                        userId INTEGER PRIMARY KEY,
                        firstName TEXT,
                        lastName TEXT
                      )'''
                   )

    # callLogs table (with FK to users table)
    cursor.execute('''CREATE TABLE IF NOT EXISTS callLogs (
        callId INTEGER PRIMARY KEY,
        phoneNumber TEXT,
        startTime INTEGER,
        endTime INTEGER,
        direction TEXT,
        userId INTEGER,
        FOREIGN KEY (userId) REFERENCES users(userId)
    )''')

    # You will implement these methods below. They just print TO-DO messages for now.
    load_and_clean_users('../../resources/users.csv')
    load_and_clean_call_logs('../../resources/callLogs.csv')
    write_user_analytics('../../resources/userAnalytics.csv')
    write_ordered_calls('../../resources/orderedCalls.csv')

    # Helper method that prints the contents of the users and callLogs tables. Uncomment to see data.
    # select_from_users_and_call_logs()

    # Close the cursor and connection. main function ends here.
    cursor.close()
    conn.close()


# TODO: Implement the following 4 functions. The functions must pass the unit tests to complete the project.


# This function will load the users.csv file into the users table, discarding any records with incomplete data
def load_and_clean_users(file_path):

    print("TODO: load_users")
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)

        for row in reader:
            if len(row) != 2:
                continue

            first_name = row[0].strip()
            last_name = row[1].strip()

            if first_name and last_name:
                cursor.execute('''
                INSERT INTO users (firstName, lastName) VALUES (?, ?)
                ''', (first_name, last_name))


# This function will load the callLogs.csv file into the callLogs table, discarding any records with incomplete data
def load_and_clean_call_logs(file_path):

    print("TODO: load_call_logs")
    
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)

        for row in reader:
            if len(row) != 5:
                continue

            phone_number  = row[0].strip()
            start_time = row[1].strip()
            end_time = row[2].strip()
            direction = row[3].strip()
            user_id = row[4].strip()

            if not (phone_number and start_time and end_time and direction and user_id):
                continue

            if not (start_time.isdigit() and end_time.isdigit() and user_id.isdigit()):
                continue

            try:
                cursor.execute('''
                INSERT INTO callLogs (phoneNumber, startTime, endTime, direction, userId)
                VALUES (?, ?, ?, ?, ?)
                ''', (phone_number, int(start_time), int(end_time), direction, int(user_id)))
            
            except sqlite3.IntegrityError:
                continue



# This function will write analytics data to testUserAnalytics.csv - average call time, and number of calls per user.
# You must save records consisting of each userId, avgDuration, and numCalls
# example: 1,105.0,4 - where 1 is the userId, 105.0 is the avgDuration, and 4 is the numCalls.
def write_user_analytics(csv_file_path):

    print("TODO: write_user_analytics")

    cursor.execute('''
 SELECT userId,
 ROUND(AVG(endTime - startTime), 1) AS avgDuration,
 COUNT(*) AS numCalls
 FROM callLogs
 GROUP BY userId
 ''')

    with open(csv_file_path, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['userId', 'avgDuration', 'numCalls'])
        for row in cursor.fetchall():
            writer.writerow([row[0], row[1], row[2]])
 
 

# This function will write the callLogs ordered by userId, then start time.
# Then, write the ordered callLogs to orderedCalls.csv
def write_ordered_calls(csv_file_path):

    print("TODO: write_ordered_calls")
    cursor.execute('''
    SELECT callId, phoneNumber, startTime, endTime, direction, userId
    FROM callLogs
    ORDER BY userId, startTime
    ''')

    with open(csv_file_path, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['callId', 'phoneNumber', 'startTime', 'endTime', 'direction', 'userId'])
        for row in cursor.fetchall():
            writer.writerow(row)



# No need to touch the functions below!------------------------------------------

# This function is for debugs/validation - uncomment the function invocation in main() to see the data in the database.
def select_from_users_and_call_logs():

    print()
    print("PRINTING DATA FROM USERS")
    print("-------------------------")

    # Select and print users data
    cursor.execute('''SELECT * FROM users''')
    for row in cursor:
        print(row)

    # new line
    print()
    print("PRINTING DATA FROM CALLLOGS")
    print("-------------------------")

    # Select and print callLogs data
    cursor.execute('''SELECT * FROM callLogs''')
    for row in cursor:
        print(row)


def return_cursor():
    return cursor


if __name__ == '__main__':
    main()
