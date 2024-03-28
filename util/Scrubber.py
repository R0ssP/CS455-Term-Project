import csv

#  subroutine
def read_index(prompt):
    while True:
        user_input = input(prompt)
        
        if user_input.strip().isdigit():
            return user_input
        else:
            print("Input type must be a number")


#  subroutine
def validate_line(line, params):
    for param in params:
        if line[int(param)] == " ":
            return False
    return True


#  subroutine
def build_output_line(lines, params):
    output_line = []
    for param in params:
        output_line.append(lines[int(param)])
    return output_line


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


#  main function
def scrubData():
    #  latitude, longitude, time dispatched, time arrived, reported as
    file_name = input("Enter the name of your file or the path if it is not in this directory ") #  NOTE - this assumes the file resides in the same directory
    file_name = file_name.strip()
    params = get_params();
 
    output_file_name = file_name[:-4] + "_out.csv"

    with open(file_name, mode='r') as file:
        csv_file = csv.reader(file)
        for lines in csv_file:
            valid = validate_line(lines, params)
            if valid:
                output_line = build_output_line(lines, params)
                with open(output_file_name, mode='a') as output_file:
                    csvwriter = csv.writer(output_file)
                    csvwriter.writerow(output_line)

scrubData()