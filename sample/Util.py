#  subroutine
from datetime import datetime
from pyspark.sql.functions import when, col
def read_index(prompt):
    while True:
        user_input = input(prompt)
        
        if user_input.strip().isdigit():
            return user_input
        else:
            print("Input type must be a number")


#  subroutine
def get_params():
    params = []

    print("For each of following values, please enter the index in which they appear in a row in your CSV file. \n *Note* the first position should map to index 0") #  edit that sentence
    reported_as = read_index("Index of type of event / reported as ")
    params.append(reported_as)

    latitude = read_index("Index of latitude ")
    longitude = read_index("Index of longitude ")
    params.append(latitude)
    params.append(longitude)

    time_dispatched = read_index("Index of dispatch time. ")
    time_arrived = read_index("Index of arrival time ")
    params.append(time_dispatched)
    params.append(time_arrived)
    return params;


def scrub_colum_array(column_list, paramIndexArray):
    for i in sorted(paramIndexArray, reverse=True):
        del column_list[int(i)]
    return column_list


def get_file():
    file_name = input("Please enter the name / path of your file: ")
    return file_name



def calculate_response_time(time_left, time_arrived):
    timestamp1 = datetime.strptime(time_left, "%m/%d/%Y %I:%M:%S %p")
    timestamp2 = datetime.strptime(time_arrived, "%m/%d/%Y %I:%M:%S %p")
    response_time_string = timestamp2 - timestamp1
    response_time_minutes = int(response_time_string.total_seconds() / 60)
    return response_time_minutes


        

