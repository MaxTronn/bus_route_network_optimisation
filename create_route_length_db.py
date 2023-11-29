# This file create a database file with columns
# ["route_id", "agency", "total_route_length", "shape_id", "route_long_name"]
# using the shapes.txt, trips.txt, routes.txt file of GTFS Data

import pandas as pd
import sqlite3

def compile_route_lengths():
    shapes_df = pd.read_csv("data/GTFS Data/shapes.txt")
    trips_df = pd.read_csv("data/GTFS Data/trips.txt")
    routes_df = pd.read_csv("data/GTFS Data/routes.txt")

    # Find the row with the maximum shape_pt_sequence for each shape_id
    max_sequence_rows = shapes_df.groupby("shape_id")["shape_pt_sequence"].idxmax()
    max_sequence_shapes = shapes_df.loc[max_sequence_rows]

    # Merge trips, max_sequence_shapes, and routes based on shape_id and route_id
    merged_df = pd.merge(trips_df, max_sequence_shapes, on="shape_id", how="inner")
    merged_df = pd.merge(merged_df, routes_df, on="route_id", how="inner")

    routes_data = []
    for route_id, route_group in merged_df.groupby("route_id"):
        # Calculate the route length for each group (i.e., each route_id)
        total_length = route_group["shape_dist_traveled"].max()
        shape_id = route_group.iloc[0]["shape_id"]
        route_long_name = route_group.iloc[0]["route_long_name"]
        agency = route_group.iloc[0]["agency_id"]

        routes_data.append({"route_id": route_id, "agency": agency, "total_route_length": total_length,
                            "shape_id": shape_id, "route_long_name": route_long_name})

    result_df = pd.DataFrame(routes_data)
    return result_df

if __name__ == "__main__":
    route_lengths_df = compile_route_lengths()
    print(route_lengths_df)

    # Save the DataFrame to a SQLite database file
    db_file_path = "route_lengths.db"
    conn = sqlite3.connect("data/route_lengths.db")
    route_lengths_df.to_sql("route_lengths_table", conn, index=False, if_exists="replace")
    conn.close()

