import math

from geographiclib.geodesic import Geodesic

def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Radius of the Earth in kilometers
    lat_diff = math.radians(lat2 - lat1)
    lon_diff = math.radians(lon2 - lon1)
    a = math.sin(lat_diff / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(lon_diff / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c
    return distance

# Corner 1
lat1, lon1 = 40.5478946903919, -73.66715128481923

# Corner 3
lat2, lon2 = 40.5478946903919, -73.65529518546721

distance = haversine(lat1, lon1, lat2, lon2)
print("Distance between opposite corners:", distance, "kilometers")


class subsquare:
    def __init__(self, attribute_list, center) -> None:
        self.topRight = attribute_list[0]
        self.topLeft = attribute_list[1]
        self.bottomLeft = attribute_list[2]
        self.bottomRight = attribute_list[3]
        self.points = attribute_list
        self.center = center


    def printPoints(self):
        print(self.points)

    def printCenter(self):
        print(self.center)
    
    def getPoints(self):
        return self.points

    def getCenter(self):
        return self.center
    


    




# make a udf for putting in the info -> very similar to other thing!
