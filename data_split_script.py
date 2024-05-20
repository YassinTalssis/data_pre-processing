import os
import pandas as pd
import csv
from itertools import combinations

def set_to_file(directory, file_name, set_):
    if not os.path.exists(directory):
        os.makedirs(directory)
    file_path = os.path.join(directory, file_name)
    union_list = [["file_name"]] + [[item] for item in set_]
    with open(file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(union_list)

# Change this to your flig-cluster file path
df1 = pd.read_csv('/kaggle/input/indexsss/flig-clusters.csv')
arc_mof_files = []
updated_files = []
filtered_filenames = df1.loc[df1['Database'] == 'ARC-MOF', 'filename']
for f in filtered_filenames:
    updated_files.append(f)

for dirname, _, filenames in os.walk('/kaggle/input/arc-data/'):
    for filename in filenames:
        arc_mof_files.append(str(filename).split('_repeat')[0] + '.cif')

print("The number of files indicated by flig_cluster:", len(updated_files))
print("The number of files in ARC-MOF:", len(arc_mof_files))
ARC_files = set(updated_files).intersection(set(arc_mof_files))
print("The intersection between ARC-DATA and flig_cluster:", len(ARC_files))

def load_and_process_csv(file_path, column_name, suffix_split='_repeat', file_extension='.cif'):
    data = pd.read_csv(file_path)
    filenames = data[column_name].values
    processed_filenames = set()
    for filename in filenames:
        if suffix_split in filename:
            processed_filenames.add(filename.split(suffix_split)[0] + file_extension)
        else:
            processed_filenames.add(filename)
    return processed_filenames

# Also change those paths
datasets = {
    "rac": set(pd.read_csv("/kaggle/input/rav-and-rdf/RACs.csv")["filename"].values),
    "rdf": load_and_process_csv("/kaggle/input/rav-and-rdf/RDFs.csv", "Structure_Name"),
    "landfill_ch4": load_and_process_csv("/kaggle/input/other-csv-files/landfill-CH4.csv", "filename"),
    "landfill_co2": load_and_process_csv("/kaggle/input/other-csv-files/landfill-CO2.csv", "filename"),
    "methane_purification_ch4": load_and_process_csv("/kaggle/input/other-csv-files/methane_purification-CH4.csv", "filename"),
    "methane_purification_co2": load_and_process_csv("/kaggle/input/other-csv-files/methane_purification-CH4.csv", "filename"),
    "geo_prop": set(pd.read_csv("/kaggle/input/geometric-properties/geometric_properties.csv")["filename"].values),
    "topology": set(pd.read_csv("/kaggle/input/all-topology-lists/all_topology_lists.csv")["Name"].values)
}

print(len(ARC_files))
datasets["ARC_files"] = ARC_files

def calculate_intersection(sets):
    if not sets:
        return set()
    return set.intersection(*sets)

results = []
base_directory = "intersections"
if not os.path.exists(base_directory):
    os.makedirs(base_directory)

# Combine the datasets for ch4 and co2
datasets["ch4"] = datasets["landfill_ch4"].union(datasets["methane_purification_ch4"])
datasets["co2"] = datasets["landfill_co2"].union(datasets["methane_purification_co2"])

# Remove the original datasets
del datasets["landfill_ch4"]
del datasets["methane_purification_ch4"]
del datasets["landfill_co2"]
del datasets["methane_purification_co2"]

columns = list(datasets.keys()) + ['Count']
intersection_info = pd.DataFrame(columns=columns)

specific_directories = {
    "D1": ["geo_prop"],
    "D2": ["geo_prop", "topology"],
    "D3-1": ["geo_prop", "topology", "rac"],
    "D3-2": ["geo_prop", "topology", "rdf"],
    "D3-3": ["geo_prop", "topology", "rac", "rdf"]
}

for directory, keys in specific_directories.items():
    sets_to_intersect = [datasets[key] for key in keys]
    intersection_result = calculate_intersection(sets_to_intersect)
    file_name = "_".join(keys) + '.csv'
    set_to_file(os.path.join(base_directory, directory), file_name, intersection_result)
    print(f"The intersection in {directory} ({', '.join(keys)}) has {len(intersection_result)} items.")
    results.append({
        'Combination': ", ".join(keys),
        'Count': len(intersection_result)
    })
    row = {key: 'yes' if key in keys else 'no' for key in datasets.keys()}
    row['Count'] = len(intersection_result)
    intersection_info = pd.concat([intersection_info, pd.DataFrame([row])], ignore_index=True)

other_intersections = []
for r in range(2, len(datasets) + 1):
    for combo in combinations(datasets.keys(), r):
        if "geo_prop" in combo and "topology" in combo:
            other_intersections.append(combo)

for idx, combo in enumerate(other_intersections, start=1):
    directory = f"D4-{idx}"
    combo_key = ', '.join(combo)
    sets_to_intersect = [datasets[key] for key in combo]
    intersection_result = calculate_intersection(sets_to_intersect)
    file_name = "_".join(combo).replace(", ", "_") + '.csv'
    set_to_file(os.path.join(base_directory, directory), file_name, intersection_result)
    print(f"The intersection of {combo_key} has {len(intersection_result)} items.")
    results.append({
        'Combination': combo_key,
        'Count': len(intersection_result)
    })
    # Add information to the DataFrame (making the table)
    row = {key: 'yes' if key in combo else 'no' for key in datasets.keys()}
    row['Count'] = len(intersection_result)
    intersection_info = pd.concat([intersection_info, pd.DataFrame([row])], ignore_index=True)

# Save the intersection to a CSV file
intersection_info.to_csv('intersection_info.csv', index=False)
print(intersection_info)
