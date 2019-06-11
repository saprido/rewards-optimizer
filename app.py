# !/usr/bin/python

import psycopg2
import os
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, String, MetaData, func
from flask_cors import CORS
from config import config
from flasgger import Swagger
from sqlalchemy.orm import sessionmaker
from flask_uploads import UploadSet, DATA, configure_uploads
from flask import Flask, flash, request, jsonify, redirect, url_for
from werkzeug.utils import secure_filename
import pandas as pd
import csv

UPLOAD_FOLDER = ''

db_string = "postgres://user:pass@cc-reward-optimizer.cefuuvy2mell.us-east-1.rds.amazonaws.com/cc-rewards"

db = create_engine(db_string)

meta = MetaData()
meta.reflect(db)

Session = sessionmaker(db)
session = Session()

credit_cards = Table('credit_cards', meta, autoload=True, autoload_with=db)
rate_rules = Table('rate_rules', meta, autoload=True, autoload_with=db)
transactions = Table('transactions', meta, autoload=True, autoload_with=db)
# user = Table('transactions.user', meta, autoload=True, autoload_with=db)


def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


cards_list = []

spending_categories_list = []


class Spending(object):
    category = ""
    amount_spent = 0.0

    # The class "constructor" - It's actually an initializer
    def __init__(self, name, amount):
        self.category = name
        self.amount_spent = amount


class CreditCard(object):
    card_name = ""
    amount_saved = 0.0
    rewardType = ""

    # The class "constructor" - It's actually an initializer
    def __init__(self, name, amount, reward_type):
        self.card_name = name
        self.amount_saved = amount
        self.rewardType = reward_type


def make_credit_card(name, amount, reward_type):
    card_dict = {
        "name": name,
        "amount_saved": amount,
        "reward_type": reward_type
    }
    return card_dict


def make_spending_entry(name, amount):
    spend_entry = {
        "category": name,
        "amount_spent": amount,
    }
    return spend_entry;


def populate_dict():
    for card in session.query(rate_rules.columns.card_id).distinct():
        print('{} : {}'.format(card.card_id, get_total_spent_not_applied(card.card_id)))
        amount_saved = round(float(get_total_saved_by_card(card.card_id)), 2)
        reward_type = session.query(credit_cards.columns.reward_type).filter(
            credit_cards.columns.card_id == card.card_id).first().reward_type
        cards_list.append(make_credit_card(card.card_id, amount_saved, reward_type))


def populate_spending(table):
    spending_categories_list.clear()
    for row in session.query(table.columns.category).distinct():
        category = row.category
        total = round(float(get_total_spent_by_category_raw(category)), 2)
        spending_categories_list.append(make_spending_entry(category, total))


def get_total_spent_by_card(card_id):
    total = 0

    for row in session.query(rate_rules.columns.category).filter(rate_rules.columns.card_id == card_id):
        category = row.category
        total += get_total_spent_by_category(category, card_id)
    return total


def make_category_info(category_name, amount_saved):
    category_info = {
        "category": category_name,
        "amount_saved": amount_saved
    }

    return category_info


def get_categories_saved(card_id):
    categories_list = {
        "name": card_id,
        "category_saved list": []
    }

    for row in session.query(rate_rules.columns.category).filter(rate_rules.columns.card_id == card_id):
        category = row.category
        category_item = make_category_info(category, get_total_saved_by_category(category, card_id))
        categories_list["category_saved list"].append(category_item)

    return categories_list


def get_total_saved_by_card(card_id):
    total = 0

    for row in session.query(rate_rules.columns.category).filter(rate_rules.columns.card_id == card_id):
        category = row.category
        total = total + get_total_saved_by_category(category, card_id)

    return total


def get_total_saved_by_category(category, card_id):
    amount_spent = get_total_spent_by_category(category, card_id)
    rate = session.query(rate_rules.columns.rate_amount).filter(rate_rules.columns.card_id == card_id,
                                                                rate_rules.columns.category == category).scalar()

    return amount_spent * rate


# return total amount spent by category from transactions db
def get_total_spent_by_category(category, card_id):
    # try:
    if category == 'ALL':
        return session.query(func.sum(transactions.columns.amount)).scalar()
        # return db.execute("SELECT SUM(amount) FROM transactions")
    elif category == 'ALL_NOT_APPLIED':
        return get_total_spent_not_applied(card_id)
    elif session.query(func.sum(transactions.columns.amount)).filter(
            transactions.columns.category == category).scalar():
        return session.query(func.sum(transactions.columns.amount)).filter(
            transactions.columns.category == category).scalar()
    else:
        return 0


# return total amount spent by category from transactions db
def get_total_spent_by_category_raw(category):
    # try:
    if session.query(func.sum(transactions.columns.amount)).filter(
            transactions.columns.category == category).scalar():
        return session.query(func.sum(transactions.columns.amount)).filter(
            transactions.columns.category == category).scalar()
    else:
        return 0


# get list of all categories (expect all not applied). Get total of each category and subtract from the total amount of
# transactions to get the total amount of money spent on non-applicable categories
def get_total_spent_not_applied(card_id):
    grand_total = get_total_spent_by_category('ALL', card_id)

    for row in session.query(rate_rules.columns.category).filter(rate_rules.columns.card_id == card_id,
                                                                 rate_rules.columns.category != 'ALL_NOT_APPLIED'):
        category = row.category
        category_total = get_total_spent_by_category(category, card_id)
        grand_total = grand_total - category_total

    return grand_total


def upload_data_from_file(file):
    params = config()
    conn = psycopg2.connect(**params)

    # expected cols: date, merchant_id, amount, category

    try:
        sql = "COPY transaction FROM STDIN DELIMITER \',\' CSV HEADER"
        f = open(file, 'r')
        cur = conn.cursor()
        cur.copy_from(f, "transactions.\"user\"", sep=',')
        conn.commit()
        f.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    return None


def deleteTable():
    params = config()
    conn = psycopg2.connect(**params)

    # expected cols: date, merchant_id, amount, category

    try:
        sql = "TRUNCATE {}; DELETE FROM {}".format("transactions.\"user\"", "transactions.\"user\"")
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    return None


if __name__ == '__main__':
    populate_dict()
    populate_spending(transactions)
    # print(get_categories_saved('SAVOR'))

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
CORS(app)
Swagger(app)
print(__name__)


@app.route('/card', methods=['POST'])
def add_new_card():
    return jsonify(request.get_json())


@app.route('/spending')
def get_books():
    return jsonify({'spending': spending_categories_list})


# @app.route('/spending/<string:table>')
# def get_books(table):
#
#     populate_spending(transactions)
#
#     return jsonify({'spending': spending_categories_list})


@app.route('/cards')
def get_cards():
    return jsonify({'cards': cards_list})


@app.route('/cards/<string:card_id>')
def get_book_by_isbn(card_id):
    return_value = {}  # empty to return just in case
    for card in cards_list:
        if card["name"] == card_id:
            return_value = {
                'card_name': card["name"],
                'amount_saved': card["amount_saved"],
                'reward_type': card["reward_type"],
                'categories': get_categories_saved(card_id)
            }
    return jsonify(return_value)


@app.route('/optimized/<string:reward_type>')
def get_optimized_for_type(reward_type):
    result = 0.0
    result_amount = 0.0
    for card in cards_list:
        if card["reward_type"] == reward_type:
            if card["amount_saved"] > result_amount:
                print("{}".format(card))
                print("{} {} {}".format(card["reward_type"], card["amount_saved"], card["name"]))
                result_amount = card["amount_saved"]
                result = card
    return jsonify(result)


@app.route('/delete')
def ghj():
    deleteTable()
    return "huzzah"


@app.route('/transactions', methods=['POST'])
def post_transactions():
    transactions_data = request.files['file']
    if transactions_data:
        filename = secure_filename(transactions_data.filename)
        path = os.getcwd()
        with open("transactions.csv", "w+") as file:
            file.write(transactions_data.read().decode("utf-8"))

        upload_data_from_file(path + "/transactions.csv")
    return "Uploaded to DB!"


# POST new credit card
# POST new credit card rule
# GET total spent on category
# GET total saved by card : would list categories


app.run(host='0.0.0.0',port=80)
