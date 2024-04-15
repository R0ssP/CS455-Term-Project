from geographiclib.geodesic import Geodesic



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
            return int(side_length)



def get_grid_edges():
    center_point = get_center()
    side_length = get_side_length()
    side_degrees = side_length / 111.32
    geod = Geodesic.WGS84
    corners = []
    for bearing in [45, 135, 225, 315]:
        result = geod.Direct(center_point[0], center_point[1], bearing, side_degrees)
        lat2 = result['lat2']
        lon2 = result['lon2']
        point = (lat2, lon2)
        corners.append(point)
    return corners;
    

