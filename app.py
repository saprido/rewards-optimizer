# !/usr/bin/python

import psycopg2
import pandas as pd
from flask import Flask, jsonify, request
from flask_cors import CORS
from config import config
from flasgger import Swagger



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


class CreditCard(object):
    card_name = ""
    amount_saved = 0.0
    rewardType = ""

    # The class "constructor" - It's actually an initializer
    def __init__(self, name, amount, reward_type):
        self.card_name = name
        self.amount_saved = amount
        self.rewardType = reward_type


def make_CreditCard(name, amount, reward_type):
    card_dict = {
        "name": name,
        "amount_saved": amount,
        "reward_type": reward_type
    }
    return card_dict


def populate_dict():
    try:
        params = config()
        conn = psycopg2.connect(**params)

        cards = pd.read_sql("SELECT DISTINCT card_id FROM rate_rules", conn).values

        for card in cards:
            card_name = '{}'.format(card[0])  # SAVOR_ONE
            amount_saved = get_total_saved_by_card("{}".format(card_name))

            reward_type = pd.read_sql("SELECT reward_type FROM credit_cards WHERE card_id='{}'".format(card_name),
                                      conn).values

            print("SELECT reward_type FROM credit_cards WHERE card_id='{}'".format(card_name))
            print(reward_type)

            cards_list.append(make_CreditCard(card_name, amount_saved[0][0], reward_type[0][0]))

    except (Exception, psycopg2.DatabaseError) as error:
        print("ERROR: populate_dict: {}".format(error))


# get total amount eligible for rewards by card
def get_total_spent_by_card(card_id):
    total = 0
    try:
        params = config()
        conn = psycopg2.connect(**params)

        categories = pd.read_sql(
            "SELECT category FROM rate_rules WHERE card_id='{}'".format(card_id), conn)

        for category in categories:
            total += get_total_spent_by_category(category, card_id)
        return total
    except (Exception, psycopg2.DatabaseError) as error:
        print("ERROR: get_total_spent_by_card: {}".format(error))


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

    try:
        params = config()
        conn = psycopg2.connect(**params)

        categories = pd.read_sql(
            "SELECT category FROM rate_rules WHERE card_id='{}'".format(card_id), conn).values

        for category in categories:
            category_item = make_category_info(category[0], get_total_saved_by_category(category[0], card_id)[0][0])
            categories_list["category_saved list"].append(category_item)

        return categories_list
    except (Exception, psycopg2.DatabaseError) as error:
        print("ERROR: get_total_saved_by_card: {}".format(error))


def get_total_saved_by_card(card_id):
    total = 0.0
    try:
        params = config()
        conn = psycopg2.connect(**params)

        categories = pd.read_sql(
            "SELECT category FROM rate_rules WHERE card_id='{}'".format(card_id), conn).values

        for category in categories:
            total = total + get_total_saved_by_category(category[0], card_id)
        return total
    except (Exception, psycopg2.DatabaseError) as error:
        print("ERROR: get_total_saved_by_card: {}".format(error))


def get_total_saved_by_category(category, card_id):
    try:
        params = config()
        conn = psycopg2.connect(**params)

        if category == 'ALL' and pd.read_sql("SELECT SUM(amount) FROM transactions", conn).values[0][0]:
            total = pd.read_sql("SELECT SUM(amount) FROM transactions", conn).values[0][0]
        elif category == 'ALL_NOT_APPLIED' and get_total_spent_not_applied(card_id):
            total = get_total_spent_not_applied(category)

        elif pd.read_sql("SELECT SUM(amount) FROM transactions WHERE category='{}'".format(category), conn).values[0]:
            total = \
                pd.read_sql("SELECT SUM(amount) FROM transactions WHERE category='{}'".format(category), conn).values[
                    0]
        else:
            return 0.0

        rate = \
            pd.read_sql(
                "SELECT rate_amount FROM rate_rules WHERE card_id='{}' AND category='{}'".format(card_id, category),
                conn).values

        return total * rate

    except (Exception, psycopg2.DatabaseError) as error:
        print("ERROR: get_total_saved_by_category: {}".format(error))


# return total amount spent by category from transactions db
def get_total_spent_by_category(category, card_id):
    try:
        params = config()
        conn = psycopg2.connect(**params)

        if category == 'ALL' and pd.read_sql("SELECT SUM(amount) FROM transactions", conn).values[0][0]:
            return pd.read_sql("SELECT SUM(amount) FROM transactions", conn).values[0][0]
        elif category == 'ALL_NOT_APPLIED' and get_total_spent_not_applied(card_id):
            return get_total_spent_not_applied(card_id)
        elif pd.read_sql("SELECT SUM(amount) FROM transactions WHERE category='{}'".format(category), conn).values[0]:
            return \
                pd.read_sql("SELECT SUM(amount) FROM transactions WHERE category='{}'".format(category), conn).values[
                    0]
        else:
            return 0.0

    except (Exception, psycopg2.DatabaseError) as error:
        print("ERROR: get_total_spent_by_category: {}".format(error))


# get list of all categories (expect all not applied). Get total of each category and subtract from the total amount of
# transactions to get the total amount of money spent on non-applicable categories
def get_total_spent_not_applied(card_id):
    try:
        params = config()
        conn = psycopg2.connect(**params)

        categories = pd.read_sql("SELECT category FROM rate_rules WHERE card_id='{}' AND NOT "
                                 "category=\'ALL_NOT_APPLIED\' AND NOT category=\'ALL\'".format(card_id), conn)

        grand_total = get_total_spent_by_category('ALL', card_id)

        for index, row in enumerate(categories.values):
            # print(index,row)
            category_total = get_total_spent_by_category("{}".format(row[0]), card_id)
            grand_total = (grand_total - category_total)

        return grand_total
    except (Exception, psycopg2.DatabaseError) as error:
        print("ERROR: get_total_spent_not_applied: {}".format(error))


if __name__ == '__main__':
    populate_dict()
    print(get_categories_saved('SAVOR'))

app = Flask(__name__)
CORS(app)
Swagger(app)
print(__name__)


@app.route('/card', methods=['POST'])
def add_new_card():
    return jsonify(request.get_json())


@app.route('/cards')
def get_books():
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
    result = 0
    result_amount = 0
    for card in cards_list:
        if card["reward_type"] == reward_type:
            if card["amount_saved"] > result_amount:
                print("{}".format(card))
                print("{} {} {}".format(card["reward_type"], card["amount_saved"], card["name"]))
                result_amount = card["amount_saved"]
                result = card
    return jsonify(result)



# POST new credit card
# POST new credit card rule
# GET total spent on category
# GET total saved by card : would list categories


app.run(port=5000)
