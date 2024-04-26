#  subroutine
from datetime import datetime


def scrub_colum_array(column_list, paramIndexArray):
    for i in sorted(paramIndexArray, reverse=True):
        del column_list[int(i)]
    column_list.remove('INCIDENT_DATE')
    return column_list


def calculate_response_time(time_left, time_arrived):
    timestamp1 = datetime.strptime(time_left, "%m/%d/%Y %I:%M:%S %p")
    timestamp2 = datetime.strptime(time_arrived, "%m/%d/%Y %I:%M:%S %p")
    response_time_string = timestamp2 - timestamp1
    response_time_minutes = int(response_time_string.total_seconds() / 60)
    return response_time_minutes