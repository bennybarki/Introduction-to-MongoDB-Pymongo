import scipy.stats as st
import pymongo
from pymongo import MongoClient
import time
import threading
import datetime
import numpy as np
import os
import pprint
import seaborn as sns
import matplotlib.pyplot as plt
from scipy.stats import norm
import random


def create_normal_dist(count, min_val, max_val, mu, sigma, bins):
    interval_list = []
    interval_len = (max_val - min_val) // bins

    for x in range(bins + 1):
        current = min_val
        interval_list.append(current, )
        min_val = min_val + interval_len

    x_z = []
    for x in range(bins):
        x_z.append(np.mean(interval_list[x:x + 2]))

    z = [(i - mu) / sigma for i in x_z]

    probs = [st.norm.cdf(x) for x in z]

    nums = []
    for x in probs:
        num = int(x * count)
        nums.append(num)
        count -= num

    final_numbers = []
    for i in range(len(interval_list) - 1):
        for j in range(nums[i]):
            final_numbers.append(random.randint(interval_list[i], interval_list[i + 1]))
    random.shuffle(final_numbers)
    return final_numbers


def get_data_from_file():
    file_name = input("Enter the path of your file: ")
    assert os.path.exists(file_name), "I did not find the file at, " + str(file_name)
    with open(file_name,'r') as file:
        ls = []
        for line in file:
            ls.append(line.rstrip().split(','))
    return ls


def insert_to_mongodb(list_of_vals_to_insert, collection_name, thread_num):
    count = 0
    for dic in list_of_vals_to_insert:
        db[collection_name].insert_one(dic)
        count += 1
    print("Thread {} inserted {} rows".format(thread_num, count))


def print_collection_docs(collection_name):
    for val in db[collection_name].find():
        pprint.pprint(val)


def plot_distribution(key_name, collection_name, bins):
    my_col = db[collection_name]
    vals_ls = []
    for x in my_col.find({}, {"_id": 0, key_name: 1, }):
        vals_ls.append(x[key_name])
    sns.distplot(vals_ls, fit=norm, bins=bins)
    plt.show()


def print_values_by_datetime(collection_name):
    for val in db[collection_name].find().sort('Ts', pymongo.ASCENDING):
        pprint.pprint(val)


def delete_one_doc(collection_name):

    user_input = input("Enter dateTime to delete  'Y-M-D H:M:S': ")
    date_time_obj = datetime.datetime.strptime(user_input,'%Y-%m-%d %H:%M:%S')
    my_col = db[collection_name]
    my_col.delete_one({'Ts': date_time_obj})


def update_one_doc(collection_name):

    user_input = input("Enter dateTime to Update  'Y-M-D H:M:S' , val1 ,val2 , val3 : ")
    val =  user_input.split(",")
    date_time_obj = datetime.datetime.strptime(val[0],'%Y-%m-%d %H:%M:%S')
    my_col = db[collection_name]

    my_col.update_many({'Ts': date_time_obj}, {'$set': {"nd_a":val[1],"nd_b":val[2],"nd_c":val[3]}})


def distinct_val(collection_name):

    user_input = input("Enter nd_x to check distinct values : ")
    x = db[COLLECTION_NAME].distinct(user_input)
    print("Number of distinct values in {} is {} ".format(user_input, len(x)))
    print(x)


# 2020-01-01 00:16:39

# MAIN


COLLECTION_NAME = 'normal'
DB_NAME = 'normal_dist'
# Connect to client
client = MongoClient("localhost", 27017)
# Use database DB_NAME
db = client[DB_NAME]


# List of lists from each line in file
ls = get_data_from_file()
# Convert each list from str to int
ls_1 = list(map(int, ls[0]))
ls_2 = list(map(int, ls[1]))
ls_3 = list(map(int, ls[2]))
# For each list, create a list of randomly distributed numbers
nums1 = create_normal_dist(ls_1[0], ls_1[1], ls_1[2], ls_1[3], ls_1[4], ls_1[5])
nums2 = create_normal_dist(ls_2[0], ls_2[1], ls_2[2], ls_2[3], ls_2[4], ls_2[5])
nums3 = create_normal_dist(ls_3[0], ls_3[1], ls_3[2], ls_3[3], ls_3[4], ls_3[5])

list_of_vals_to_insert = []
date_time_str = '2020-01-01 00:00:00'
date_time_obj = datetime.datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S')
for x, y, z in zip(nums1, nums2, nums3):
    dic = {"Ts": date_time_obj,
           "nd_a": x,
           "nd_b": y,
           "nd_c": z}
    date_time_obj += datetime.timedelta(seconds=1)
    list_of_vals_to_insert.append(dic)


# Remove existing data from collection if the collection is not empty
if db[COLLECTION_NAME].count() != 0:
    db[COLLECTION_NAME].remove({})

t1 = time.time()

thread1 = threading.Thread(target=insert_to_mongodb,args=(list_of_vals_to_insert[0:250], COLLECTION_NAME, "1"))
thread2 = threading.Thread(target=insert_to_mongodb,args=(list_of_vals_to_insert[250:500], COLLECTION_NAME, "2"))
thread3 = threading.Thread(target=insert_to_mongodb,args=(list_of_vals_to_insert[500:750], COLLECTION_NAME, "3"))
thread4 = threading.Thread(target=insert_to_mongodb,args=(list_of_vals_to_insert[750:1000], COLLECTION_NAME, "4"))

thread1.start()
thread2.start()
thread3.start()
thread4.start()
# This ensures that each has finished processing the tasks
thread1.join()
thread2.join()
thread3.join()
thread4.join()

print("Done in: ", (time.time()-t1), " seconds.")

# Functions #

#MongoDB Delete Document
# delete_one_doc(COLLECTION_NAME)

#MongoDb update Document
#update_one_doc(COLLECTION_NAME)

#MongoDb distinct
#distinct_val(COLLECTION_NAME)

# Print collection docs by datetime key ascending
#print_values_by_datetime(COLLECTION_NAME)

# Print all docs of a collection
# print_collection_docs(COLLECTION_NAME)

# Print collection statistics
#pprint.pprint(db.command("collstats", COLLECTION_NAME))

# Plot distribution of column values in collection
plot_distribution("nd_a", COLLECTION_NAME, ls_1[5])
plot_distribution("nd_b", COLLECTION_NAME, ls_2[5])
plot_distribution("nd_c", COLLECTION_NAME, ls_3[5])