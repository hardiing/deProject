from urllib.request import urlopen
from bs4 import BeautifulSoup
from flask import Flask, render_template, jsonify
from flask_mysqldb import MySQL
import MySQLdb.cursors
import pandas as pd

app = Flask(__name__)
app.config['MYSQL_HOST'] = 'localhost'
app.config['MYSQL_USER'] = 'nathan'
app.config['MYSQL_PASSWORD'] = 'hannat'
app.config['MYSQL_DB'] = 'football'

mysql = MySQL(app)

@app.route('/', methods=['GET', 'POST'])
def main():
    return render_template('index.html')  # return web page with MySQL data


@app.route('/win_trends', methods=['GET', 'POST'])
def win_trends():
    win_trends = 'https://www.teamrankings.com/nfl/trends/win_trends/'
    html = urlopen(win_trends)
    soup = BeautifulSoup(html, features="html.parser")

    headers = [th.getText() for th in soup.find_all('tr')[0].findAll('th')]
    rows = soup.find_all('tr')[1:]

    table_data = [[td.getText() for td in rows[i].find_all('td')] for i in range(len(rows))]

    win_df = pd.DataFrame(table_data, columns=headers)
    win_df.to_csv('win_trends.csv', index=False)

    cur = mysql.connection.cursor()
    cur.execute('DROP TABLE IF EXISTS win_trends;')
    print('Creating table...')
    cur.execute('CREATE TABLE win_trends (team_name VARCHAR(255), win_loss VARCHAR(255), win_percentage VARCHAR(255), margin_of_victory INT, ats_plus_minus INT);')
    print('Table is created...')
    for i, row in win_df.iterrows():
        sql = 'INSERT INTO football.win_trends VALUES (%s,%s,%s,%s,%s)'
        cur.execute(sql, tuple(row))
        print('Record inserted')
        mysql.connection.commit()

    headings = ("Team", "Win/Loss Record", "Win Percentage", "Margin of Victory", "Against the Spread +/-")
    cursor = mysql.connection.cursor(MySQLdb.cursors.DictCursor)  # create variable for connection
    cursor.execute("SELECT * FROM win_trends")  # execute query
    data = cursor.fetchall()  # fetch all from database
    cursor.close()
    return render_template('win_trends.html', headings=headings, data=data)  # return web page with MySQL data

@app.route('/ats_trends', methods=['GET', 'POST'])
def ats_trends():
    ats_trends = 'https://www.teamrankings.com/nfl/trends/ats_trends/'
    html = urlopen(ats_trends)
    soup = BeautifulSoup(html, features="html.parser")

    headers = [th.getText() for th in soup.find_all('tr')[0].find_all('th')]
    rows = soup.find_all('tr')[1:]

    table_data = [[td.getText() for td in rows[i].find_all('td')] for i in range(len(rows))]

    ats_df = pd.DataFrame(table_data, columns=headers)
    ats_df.to_csv('ats_trends.csv', index=False)

    cur = mysql.connection.cursor()
    cur.execute('DROP TABLE IF EXISTS ats_trends;')
    print('Creating table...')
    cur.execute('CREATE TABLE ats_trends (team_name VARCHAR(255), ats_record VARCHAR(255), cover_percentage VARCHAR(255), margin_of_victory INT, ats_plus_minus INT);')
    print('Table is created...')
    for i, row in ats_df.iterrows():
        sql = 'INSERT INTO football.ats_trends VALUES (%s,%s,%s,%s,%s)'
        cur.execute(sql, tuple(row))
        print('Record inserted')
        mysql.connection.commit()

    headings = ("Team", "ATS Record", "Cover Percentage", "Margin of Victory", "Against the Spread +/-")
    cursor = mysql.connection.cursor(MySQLdb.cursors.DictCursor)  # create variable for connection
    cursor.execute("SELECT * FROM ats_trends")  # execute query
    data = cursor.fetchall()  # fetch all from database
    cursor.close()
    return render_template('ats_trends.html', headings=headings, data=data)  # return web page with MySQL data

@app.route('/ou_trends', methods=['GET', 'POST'])
def ou_trends():
    ou_trends = 'https://www.teamrankings.com/nfl/trends/ou_trends/'
    html = urlopen(ou_trends)
    soup = BeautifulSoup(html, features="html.parser")

    headers = [th.getText() for th in soup.find_all('tr')[0].find_all('th')]
    rows = soup.find_all('tr')[1:]

    table_data = [[td.getText() for td in rows[i].find_all('td')] for i in range(len(rows))]

    ou_df = pd.DataFrame(table_data, columns=headers)
    ou_df.to_csv('ou_trends.csv', index=False)

    cur = mysql.connection.cursor()
    cur.execute('DROP TABLE IF EXISTS ou_trends;')
    print('Creating table...')
    cur.execute('CREATE TABLE ou_trends (team_name VARCHAR(255), over_record VARCHAR(255), over_percentage VARCHAR(255), under_percentage VARCHAR(255), total_plus_minus INT);')
    print('Table is created...')
    for i, row in ou_df.iterrows():
        sql = 'INSERT INTO football.ou_trends VALUES (%s,%s,%s,%s,%s)'
        cur.execute(sql, tuple(row))
        print('Record inserted')
        mysql.connection.commit()

    headings = ("Team", "Over Record", "Over Percentage", "Under Percentage", "Total +/-")
    cursor = mysql.connection.cursor(MySQLdb.cursors.DictCursor)  # create variable for connection
    cursor.execute("SELECT * FROM ou_trends")  # execute query
    data = cursor.fetchall()  # fetch all from database
    cursor.close()
    return render_template('ou_trends.html', headings=headings, data=data)  # return web page with MySQL data

if __name__ == '__main__':
    app.run(debug=True, port=5000)


