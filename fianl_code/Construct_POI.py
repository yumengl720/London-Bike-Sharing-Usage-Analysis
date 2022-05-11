import pandas as pd
import numpy as np
import time
import googlemaps



API_KEY = open('apikey.txt', 'r').read()
map_client = googlemaps.Client(API_KEY)

def get_places(location):
  # create an empty list to store results
  search_list = []

  # extract result without a search term
  response = map_client.places_nearby(
            location = location,
            radius = distance)

  search_list.extend(response.get('results'))
  next_page_token = response.get('next_page_token')

  # obtain next_page result
  while next_page_token:
      time.sleep(2)
      response = map_client.places_nearby(
      location = location,
      radius = distance,
      page_token = next_page_token
      )
      search_list.extend(response.get('results'))
      next_page_token = response.get('next_page_token')
  df = pd.DataFrame(search_list)
  return df['types']

distance = 100

geo=pd.read_csv('london_stations.csv')
geo_tuples = tuple(zip(geo.latitude, geo.longitude))

test=list(geo_tuples)
df=list(map(get_places, test))

def extract_features(x):
  return x.map(lambda x : x[0])
r = pd.DataFrame(map(extract_features, df)).reset_index()
r = r.T
r

from numpy import NaN
ref_dict = {"food": "food", "restaurant": "food", "cafe":"food", "store" : "mall", "supermarket": "mall","convenience_store":"mall", "home_goods_store":"mall", "clothing_store":"mall", "bakery":"mall", "hair_care":"mall", "grocery_or_supermarket":"mall", "car_repair" :"mall", "electronics_store":"mall", "beauty_salon":"mall", "furniture_store":"mall", "pharmacy":"mall", "travel_agency":"mall", "jewelry_store":"mall", "shoe_store":"mall", "meal_takeaway":"mall", "florist":"mall", "department_store":"mall", "car_dealer":"mall", "shopping_mall":"mall", "meal_delivery":"mall", "book_store":"mall", "liquor_store":"mall", "laundry":"mall", "pet_store":"mall", "gas_station":"mall", "bicycle_store":"mall", "hardware_store":"mall", "car_rental":"mall", "locksmith":"mall", "electrician":"mall", "plumber":"mall", "car_wash":"mall", "movie_rental":"mall",
"finance":"business", "atm":"business", "bank":"business", "real_estate_agency":"business", "general_contractor":"business", "insurance_agency":"business", "accounting":"business", "moving_company":"business", "storage":"business", "lawyer":"business", "premise":"business", "funeral_home":"business", "cemetery":"business", "drugstore":"business", "roofing_contractor": "business",
"health" :"hospital", "dentist":"hospital", "doctor":"hospital", "hospital":"hospital", "veterinary_care":"hospital", "physiotherapist":"hospital",
"gym": "sport", "stadium": "sport","bowling_alley":"sport",
    
"bar":     "entertainment",
"spa":     "entertainment",
"night_club":  "entertainment",
"movie_theater":"entertainment",
"casino": "entertainment",

"lodging": "apartment",

"park":       "park",
"tourist_attraction": "park",
"place_of_worship": "park",
"library":      "park",
"church":      "park",
"museum":      "park",
"art_gallery":    "park",
"aquarium":     "park",
"painter":      "park",
"hindu_temple":   "park",
"amusement_park":  "park",

"school":         "government",
"primary_school": "government",
"local_government_office": "government",
"police":          "government", 
"post_office":       "government",
"university":        "government",
"embassy":         "government",
"fire_station":       "government",
"courthouse":        "government",
"city_hall": "government",
"secondary_school": "government",
"zoo": "government",
"mosque": "government",
"synagogue": "government",

"transit_station":"transportation",
"taxi_stand": "transportation",
"bus_station":  "transportation",
"parking":    "transportation",
"subway_station": "transportation",
"airport": "transportation",
"light_rail_station": "transportation",
"train_station": "transportation",
"route": "transportation",
            
"point_of_interest": np.NAN,

"sublocality_level_1": "locality",
"neighborhood":"locality"}

ready_to_count=r.replace(ref_dict).iloc[1:,]

count = ready_to_count.apply(pd.Series.value_counts,axis=0).fillna(0)
count.to_csv("count.csv")
