from geographiclib.geodesic import Geodesic
import math

def get_center():
    params = []
    while True:
        latitude = input("Please enter the latitude for the center of your city. \n For example, '29.7604' ")
        try:
            params.append(float(latitude))
            break
        except ValueError:
            print("ERROR: Input was not a decimal.")
        
    while True:
        longitude = input("Please enter the longitude for the center of your city. \n For example, '29.7604' ")
        try:
            params.append(float(longitude))
            break
        except ValueError:
            print("ERROR: Input was not a decimal.")
    return params
    

def get_side_length():
    while True:
        side_length = input("Please enter an integer value that represents the desired side length of the logical grid in kilometers. \n For example, '55' ")
        if(side_length.isdigit()):
            # calculate the distance desired from the start point
             # pythag
            return int(side_length)



def get_grid_edges(side_length_in_km):
    center_point = get_center()
    side_length_in_meters = (side_length_in_km * 1000) / math.sqrt(2)
    geod = Geodesic.WGS84
    corners = []
    for bearing in [45, 135, 225, 315]:
        result = geod.Direct(center_point[0], center_point[1], bearing, side_length_in_meters)
        lat2 = result['lat2']
        lon2 = result['lon2']
        point = (lat2, lon2)
        corners.append(point)
    return corners
    

def generate_grid():
    # top left corner is the second tuple
    side_length = get_side_length()
    corners = get_grid_edges(side_length)
    # corners = get_grid_edges()
    geod = Geodesic.WGS84
    top_left_offset = corners[1]
    distance_to_center = 1000 / math.sqrt(2)
    center_point_result = geod.Direct(top_left_offset[0], top_left_offset[1], 315, 1000)
    center_lat = center_point_result['lat2']
    center_long = center_point_result['lon2']

    number_of_cells = side_length * side_length

    lat_offset_km = (-1 / 111)
    lon_offset_km = (1 / (111 * math.cos(math.radians(center_lat))))   # negative value to move DOWN

    grid = []

    for i in range(number_of_cells):
        points = []
        new_center_lat = center_lat + (i // side_length) * lat_offset_km
        if i == 0:
            new_center_lon = center_long
        else:
            new_center_lon = center_long + (i % side_length) * lon_offset_km

        new_center_point = [new_center_lat, new_center_lon]
        for bearing in [45, 135, 225, 315]:
            result = geod.Direct(new_center_point[0], new_center_point[1], bearing, distance_to_center)
            lat2 = result['lat2']
            lon2 = result['lon2']
            point = (lat2, lon2)
            points.append(point)
        grid.append(points)

    return grid