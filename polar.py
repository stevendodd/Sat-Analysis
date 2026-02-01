import matplotlib.pyplot as plt
import numpy as np
import os
import re
import csv
import json
from datetime import datetime
from pathlib import Path

# Directory containing the CSV and JSON files
directory = "./echo_passes"

# Markers and colors
markers = ['o', 'v', '^', '<', '>', 's', 'p', 'P', '*', 'h', 'H', 'D', 'd', 'X', '+', 'x', '|', '_', '1', '2', '3', '4', '8']
color_list = plt.cm.tab20.colors

satellite_list = []
data = {}  # satellite -> pass_name -> info (decode points)

# Load decode points from CSVs
for filename in os.listdir(directory):
    if not filename.lower().endswith(".csv"):
        continue

    match = re.match(r"(\d{8})_(\d{6})_(.+)\.csv", filename, re.IGNORECASE)
    if not match:
        print(f"Skipping {filename}: filename format not recognized")
        continue

    date_str, time_str, sat_name_raw = match.groups()
    sat_name = sat_name_raw.strip().upper()

    try:
        pass_date = datetime.strptime(date_str + time_str, "%Y%m%d%H%M%S")
        pass_label = f"{sat_name} {pass_date.strftime('%Y-%m-%d %H:%M')}"
    except:
        pass_label = f"{sat_name} {date_str}_{time_str}"

    filepath = Path(directory) / filename

    az_list = []
    el_list = []

    with open(filepath, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip header
        for row in reader:
            if len(row) < 10:
                continue
            try:
                az = float(row[8])   # Az column
                el = float(row[9])   # El column
                az_list.append(az)
                el_list.append(el)
            except ValueError:
                continue

    if len(az_list) == 0:
        print(f"Skipping {filename}: no valid decode points")
        continue

    if sat_name not in satellite_list:
        satellite_list.append(sat_name)

    color_index = satellite_list.index(sat_name)
    color = color_list[color_index % len(color_list)]

    if sat_name not in data:
        data[sat_name] = {}
    pass_count = len(data[sat_name])
    marker = markers[pass_count % len(markers)]

    data[sat_name][pass_label] = {
        "color": color,
        "marker": marker,
        "az": az_list,
        "el": el_list,
        "date": pass_date,
        "filename": filename
    }

# Prepare plot
if not data:
    print("No valid data found.")
else:
    fig = plt.figure(figsize=(14, 14))
    ax = fig.add_subplot(111, projection="polar")

    ax.set_theta_zero_location("N")
    ax.set_theta_direction(-1)
    ax.set_ylim(0, 90)

    ax.set_yticks([0, 15, 30, 45, 60, 75, 90])
    ax.set_yticklabels(["90°", "75°", "60°", "45°", "30°", "15°", "0°"])

    angles = np.arange(0, 360, 30)
    labels = ["N", "30°", "60°", "E", "120°", "150°", "S", "210°", "240°", "W", "300°", "330°"]
    ax.set_thetagrids(angles, labels)

    ax.grid(True, linestyle="--", alpha=0.7)

    # Plot each pass
    for sat_name, passes in data.items():
        for pass_name, info in passes.items():
            # Decode points (scatter)
            az_rad = np.deg2rad(info["az"])
            r = 90 - np.array(info["el"])
            ax.scatter(
                az_rad, r,
                color=info["color"],
                marker=info["marker"],
                s=100,
                label=f'{pass_name} ({len(info["az"])} decodes)',
                edgecolors="black",
                linewidth=0.7,
                zorder=3
            )

            # Load corresponding real track from JSON
            base_name = info["date"].strftime('%Y%m%d_%H%M%S') + f"_{sat_name}"
            json_path = Path(directory) / f"{base_name}.json"

            if json_path.exists():
                try:
                    with open(json_path, 'r', encoding='utf-8') as f:
                        track = json.load(f)
                    points = track["points"]
                    if points:
                        track_az = [p["az"] for p in points]
                        track_el = [p["el"] for p in points]
                        track_az_rad = np.deg2rad(track_az)
                        track_r = 90 - np.array(track_el)
                        ax.plot(track_az_rad, track_r,
                                color=info["color"],
                                linewidth=2.5,
                                alpha=0.8,
                                #label=f'{pass_name} track'
                                )
                except Exception as e:
                    print(f"Error reading track {json_path}: {e}")
            else:
                print(f"No real track JSON found for {pass_name}")

    ax.legend(loc="upper left", bbox_to_anchor=(1.1, 1.0), fontsize=9)

    satellites_plotted = ", ".join(data.keys()) or "None"
    ax.set_title(
        f"Successful Self-Decodes on {satellites_plotted}\n"
        "Points: echo decode positions",
        fontsize=14,
        pad=20,
    )

    plt.tight_layout()
    plt.show()