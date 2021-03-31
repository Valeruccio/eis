# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from datetime import datetime

import faker
from pymongo import MongoClient
from pprint import pprint
import random
import string
from time import sleep

fake = faker.Faker()


operation_types = ['read', 'create', 'update', 'delete']


def get_random_operation():
    return operation_types[random.randint(0, len(operation_types) - 1)]


def get_random_string(length):
    letters = string.ascii_letters
    result_str = ''.join(random.choice(letters) for i in range(length))

    return result_str


def drop_accounts(accounts_collection):
    accounts_collection.delete_many({})


def generate_accounts():
    accounts = []
    for i in range(10):
        account = {
            'number': random.randint(1000000000000, 9999999999999),
            'name': "Пользователь №" + str(i),
            'sessions': []
        }

        for e in range(random.randint(1, 4)):
            session = {
                'created_at': datetime.utcnow(),
                'session_id': get_random_string(50),
                'actions': [],
            }

            for j in range(random.randint(1, 4)):
                session['actions'].append({
                    'type': get_random_operation(),
                    'created_at': datetime.utcnow()
                })
                sleep(1)
            account['sessions'].append(session)

        accounts.append(account)

    return accounts


def get_aggregated_accounts(accounts_collection):
    accounts = []
    for account in accounts_collection.find():
        actions_count = {}
        action_dates = {}
        action_types = []

        for session in account['sessions']:
            for action in session['actions']:
                t = action['type']
                if t not in action_types:
                    action_types.append(t)

                if t not in actions_count:
                    actions_count[t] = 0
                actions_count[t] += 1

                if t in action_dates:
                    if action['created_at'] > action_dates[t]:
                        action_dates[t] = action['created_at']
                else:
                    action_dates[t] = action['created_at']

        actions = []
        for action_type in action_types:
            actions.append({
                'type': action_type,
                'last': action_dates[action_type],
                'count': actions_count[action_type]
            })
        accounts.append({
            'number': account['number'],
            'actions': actions
        })

    return accounts


def main():
    client = MongoClient("mongodb://localhost:27016/")
    db = client.eis
    accounts_collection = db.Accounts

    # раскомментировать для пересоздания аккаунтов
    # drop_accounts(accounts_collection)
    # accounts = generate_accounts()
    # accounts_collection.insert_many(accounts)

    aggregated = get_aggregated_accounts(accounts_collection)
    pprint(aggregated)


if __name__ == '__main__':
    main()
