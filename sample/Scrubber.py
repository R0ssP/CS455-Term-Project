#  subroutine
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
    question = input("Does your data contain 'zipcode' or 'latitude / longitude' ")

    if question.strip() == 'zipcode':
        zipcode = read_index("index of zipcode ")
        params.append(zipcode)
    elif question.strip() == 'latitude / longitude':
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
