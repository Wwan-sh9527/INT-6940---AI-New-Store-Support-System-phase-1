

import os
import math
import pandas as pd
import json
from pyproj import Transformer


#------------------------------------------------------------------------------------------------------------------
#A. Data Initialization
#Load the following files provided in class:

cbgs = pd.read_csv("worcester_cbgs.csv")
pois = pd.read_csv("worcester_pois.csv")
visits = pd.read_csv("worcester_cbg_poi_visits.csv")
params = pd.read_csv("calibrated_parameters_filtered.csv")
distance_matrix = pd.read_csv("worcester_cbg_poi_distance.csv")

#------------------------------------------------------------------------------------------------------------------
#B. User Input Layer
# Added non-sense input prevention

category_lookup = pois[["top_category", "naics_code"]].dropna().copy()
category_lookup["naics_code"] = category_lookup["naics_code"].astype(str).str.strip()
category_lookup["top_category"] = category_lookup["top_category"].astype(str).str.strip()
category_lookup = category_lookup.drop_duplicates()

# Latitude
try:
    lat = float(input("Enter latitude: "))
except ValueError:
    print("Error: Latitude must be a number. ")
    exit()
if not (-90 <= lat <= 90):
    print("Error: Latitude must be between -90 and 90.")
    exit()

# Longitude
try:
    lon = float(input("Enter longitude: "))
except ValueError:
    print("Error: Longitude must be a number.")
    exit()
if not (-180 <= lon <= 180):
    print("Error: Longitude must be between -180 and 180.")
    exit()

# Top Category
def naics_to_category(user_naics, lookup_df):
    user_naics = str(user_naics).strip()
    matches = lookup_df.loc[lookup_df["naics_code"] == user_naics, "top_category"].unique()
    return matches.tolist() if len(matches) > 0 else None
def category_to_naics(user_category, lookup_df):
    user_category = str(user_category).strip().lower()
    matches = lookup_df.loc[
        lookup_df["top_category"].str.lower() == user_category,
        "naics_code"
    ].unique()
    return matches.tolist() if len(matches) > 0 else None
user_input = input("Enter NAICS code or top category: ").strip()
if user_input.isdigit():
    matched_categories = naics_to_category(user_input, category_lookup)
    
    if matched_categories:
        print(f"\nInput NAICS code: {user_input}")
        print("\nMatched top category/categories:")
        for cat in matched_categories:
            print(f" - {cat}")
        
        top_category = matched_categories[0]
        naics_code = user_input
    else:
        print(f"\nNo top category found for NAICS code: {user_input}")
        print("\nPlease enter full and correct name of the top category!!!")
        exit()
else:
    matched_naics = category_to_naics(user_input, category_lookup)
    
    if matched_naics:
        print(f"\nInput top category: {user_input}")
        print("\nMatched NAICS code(s):")
        for code in matched_naics:
            print(f" - {code}")
        
        top_category = user_input
        naics_code = matched_naics[0]
    else:
        print(f"\nNo NAICS code found for top category: {user_input}")
        print("\nPlease enter correct Naics!!!")
        exit()

# Store size
try:
    new_store_size = float(input("\nEnter store size (square meters): "))
except ValueError:
    print("Error: Store size must be a number.")
    exit()
if new_store_size <= 0:
    print("Error: Store size must be greater than 0.")
    exit()


#------------------------------------------------------------------------------------------------------------------
#C. The Distance Calculation
#Since the new store is not in the provided distance matrix, you must implement a Function to calculate the distance (in meters) between the new store's coordinates and every Census Block Group (CBG) centroid in worcester_cbgs.csv.
#Use Straight-Line Euclidean Distances calculated via a Projected Coordinate System (Latitude/Longitude in WGS84).

visits["visitor_home_cbg"] = visits["visitor_home_cbg"].astype(str)
distance_matrix["GEOID10"] = distance_matrix["GEOID10"].astype(str)
pois["naics_code"] = pois["naics_code"].astype(str)
params["NAICS code"] = params["NAICS code"].astype(str)

# Find alpha and beta
selected_params = params[params["NAICS code"].astype(str) == str(naics_code)]


alpha = selected_params.iloc[0]["alpha"]
beta = selected_params.iloc[0]["beta"]


# Find Competitors in the same category, and merge competitor size with distance matrix
same_category_pois = pois[pois["naics_code"].astype(str) == str(naics_code)]

same_category_distances = distance_matrix.merge(
    same_category_pois[["placekey", "wkt_area_sq_meters"]],
    on="placekey",
    how="inner"
)


# Convert columns to numeric
same_category_distances["distance_m"] = pd.to_numeric(
    same_category_distances["distance_m"]
)

same_category_distances["wkt_area_sq_meters"] = pd.to_numeric(
    same_category_distances["wkt_area_sq_meters"],
    errors="coerce"
)

# Remove invalid rows
same_category_distances = same_category_distances.dropna(
    subset=["distance_m", "wkt_area_sq_meters"]
)

same_category_distances = same_category_distances[
    same_category_distances["distance_m"] > 0
]

# Calculate each competitor's utility for each CBG
same_category_distances["utility_existing_each"] = (
    same_category_distances["wkt_area_sq_meters"] ** alpha
) / (
    same_category_distances["distance_m"] ** beta
)


# Sum all competitors' utility by CBG
existing_utility_by_cbg = same_category_distances.groupby(
    "GEOID10",
    as_index=False
)["utility_existing_each"].sum()

existing_utility_by_cbg = existing_utility_by_cbg.rename(
    columns={"utility_existing_each": "total_existing_utility"}
)

with open("worcester_cbgs_map.geojson", "r", encoding="utf-8") as f:
    cbg_geo = json.load(f)


# Build a CBG centroid table from geojson
cbg_centroids = []

for feature in cbg_geo["features"]:
    props = feature["properties"]

    cbg_centroids.append({
        "GEOID10": str(props["GEOID10"]),
        "centroid_lat": float(props["INTPTLAT10"]),
        "centroid_lon": float(props["INTPTLON10"])
    })


#Calculate distance in meters between two latitude/longitude points.
cbg_centroids = pd.DataFrame(cbg_centroids)

transformer = Transformer.from_crs(
    "EPSG:4326",
    "EPSG:26986",
    always_xy=True
)

def projected_euclidean_distance_m(lat1, lon1, lat2, lon2):
    x1, y1 = transformer.transform(lon1, lat1)
    x2, y2 = transformer.transform(lon2, lat2)
    return math.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)

cbg_centroids["new_store_distance_m"] = cbg_centroids.apply(
    lambda row: projected_euclidean_distance_m(
        lat, lon,
        row["centroid_lat"], row["centroid_lon"]
    ),
    axis=1
)

# Calculate distance from the new store to each CBG centroid
cbg_centroids = cbg_centroids[cbg_centroids["new_store_distance_m"] > 0]

cbg_centroids["utility_new"] = (new_store_size ** alpha) / ( cbg_centroids["new_store_distance_m"] ** beta)

# Merge new store utility with existing utility
cbg_centroids["GEOID10"] = cbg_centroids["GEOID10"].astype(str)
existing_utility_by_cbg["GEOID10"] = existing_utility_by_cbg["GEOID10"].astype(str)

combined = cbg_centroids.merge(
    existing_utility_by_cbg,
    on="GEOID10",
    how="left"
)


#------------------------------------------------------------------------------------------------------------------
#D. The Huff Model Logic

combined["total_existing_utility"] = combined["total_existing_utility"].fillna(0)

# Calculate probability of choosing the new store
combined["P_new"] = combined["utility_new"] / (combined["utility_new"] + combined["total_existing_utility"])


#------------------------------------------------------------------------------------------------------------------
#E. Demand Estimation
#Calculate the Total Category Demand for each CBG by summing all historical visits in that category from that CBG.
#Predicted Visits = Pnew x Total Category Demand

# Make sure key columns are the same type
visits["placekey"] = visits["placekey"].astype(str)
visits["visitor_home_cbg"] = visits["visitor_home_cbg"].astype(str)
pois["placekey"] = pois["placekey"].astype(str)
pois["naics_code"] = pois["naics_code"].astype(str)

# Merge visits with POI category info
visits_with_category = visits.merge(
    pois[["placekey", "naics_code"]],
    on="placekey",
    how="left"
)


# Keep only visits in the same category as the new store
same_category_visits = visits_with_category[
    visits_with_category["naics_code"] == str(naics_code)
]


# Sum total category demand by home CBG
demand_by_cbg = same_category_visits.groupby(
    "visitor_home_cbg",
    as_index=False
)["visit_count"].sum()

demand_by_cbg = demand_by_cbg.rename(
    columns={
        "visitor_home_cbg": "GEOID10",
        "visit_count": "total_demand"
    }
)


# Make sure GEOID10 is string for merge
demand_by_cbg["GEOID10"] = demand_by_cbg["GEOID10"].astype(str)


# Merge demand into combined table
combined = combined.merge(
    demand_by_cbg,
    on="GEOID10",
    how="left"
)

combined["total_demand"] = combined["total_demand"].fillna(0)



# Final predicted visits
combined["predicted_visits"] = combined["P_new"] * combined["total_demand"]


# Total predicted visits to the new store
total_predicted_visits = combined["predicted_visits"].sum()


# === Output: Summary of Results ===
print("\n" + "-"*28 + "Summary of New Store Site Prediction Result:" + "-"*28)

print("\nParameters:")
print(f"Top Category = {top_category}")
print(f"  Category (NAICS): {naics_code}")
print(f"  Alpha: {alpha}")
print(f"  Beta: {beta}")

print("\nNumber of competitors:", len(same_category_pois))

print(f"\nNumber of competitors: {combined["total_demand"].sum():.0f}")

print(f"\nTotal predicted visits to new store: {total_predicted_visits:.2f}")

print("\n" + "-"*100)

"""
#Latitude & Longitude: (e.g., 42.27, -71.80)
#Category: (Top Category or NAICS Code e.g. 445310)
#Store Size: (Square Meters e.g. 2500)
"""