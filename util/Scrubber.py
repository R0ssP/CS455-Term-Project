import csv


def scrubData():
    print("hi")
    #  latitude, longitude, time dispatched, time arrived, reported as
    file_name = input("Enter the name of your file ")
    params = []

    print("For each of following values, please enter the index in which they appear in a row in your CSV file. ") #  edit that sentence
    reported_as = read("Index of type of crime / what it was reported as ")
    params.append(reported_as)
    question = input("Does your data contain 'zipcode' or 'latitude / longitude' ")

    

    #modularize that
    if question == 'zipcode':
        zipcode = read("index of zipcode ")
        params.append(zipcode)
    elif question == 'latitude / longitude':
        latitude = read("Index of latitude ")
        longitude = read("Index of longitude ")
        params.append(latitude)
        params.append(longitude)
    time_dispatched = read("Index of dispatch time. ")
    time_arrived = read("Index of arrival time ")
    params.append(time_dispatched)
    params.append(time_arrived)

    print(params) 
 
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
            

def read(prompt):
    while True:
        user_input = input(prompt)

        if user_input.isdigit():
            return user_input
        else:
            print("Input type must be a number")

def validate_line(line, params):
    for param in params:
        if line[int(param)] == " ":
            return False
    return True

def build_output_line(lines, params):
    output_line = []
    for param in params:
        output_line.append(lines[int(param)])
    return output_line


scrubData();

