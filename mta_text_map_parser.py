import requests
import pandas as pd
from bs4 import BeautifulSoup
import json
import time
import io

base_url = "https://www.mta.info"
subway_map_url = "https://www.mta.info/maps/subway-line-maps"

# Download MTA_Subway_Stations from NY Open Data as CSV
csv_url = "https://data.ny.gov/api/v3/views/39hk-dx4f/export.csv?accessType=DOWNLOAD&app_token=bHWsGtRFRP9x8Hl8lYivqM1hQ"
stations_csv = requests.get(csv_url)
stations_df = pd.read_csv(io.StringIO(stations_csv.text))
print(f"Downloaded {len(stations_df)} stations from CSV.")

# Use a session object to persist cookies and headers
session = requests.Session()

response = session.get(subway_map_url)
soup = BeautifulSoup(response.content, "html.parser")

# Find all anchor tags whose href attribute starts with the specified path
subway_line_links = soup.select('a[href^="/maps/subway-line-maps/"], a[href^="https://new.mta.info/maps/subway-line-maps/"]')

all_lines_data = {} # Dictionary to store all scraped data
processed_urls = []

# Process and normalize URLs before scraping
for link in subway_line_links:
    href = link.get('href')
    # Get the part of the URL after the domain
    path_part = href.split('/maps/subway-line-maps/')[1]
    # Construct the URL with 'new.mta.info' and lowercase line identifier
    normalized_url = f"https://new.mta.info/maps/subway-line-maps/{path_part.lower()}"
    if normalized_url not in processed_urls:
        processed_urls.append(normalized_url)

# Print the extracted line identifiers
print(f"{len(processed_urls)} Line URLs found and normalized.")

# Visit each link and scrape the station data
for page_url in processed_urls:
    session.cookies.clear() # Clear cookies to prevent redirect loops
    page_response = None
    for attempt in range(5): # Retry up to 5 times
        try:
            page_response = session.get(page_url)
            page_response.raise_for_status() # Raise an exception for bad status codes
            break # If successful, exit the retry loop
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt + 1} failed for {page_url}: {e}")
            if attempt < 4:
                time.sleep(1) # Wait 1 second before retrying
            else:
                print(f"Could not process {page_url} after 5 attempts.")
    
    if not page_response or not page_response.ok:
        continue # Skip to the next link if all retries failed

    page_soup = BeautifulSoup(page_response.content, "html.parser")
    
    # Extract the train line name from the URL for the dictionary key
    train_line = page_url.split('/')[-1][0].upper()
    print(f"--- Scraping {train_line} Line---")
    all_lines_data[train_line] = {}

    # Find all divs that contain a table
    table_sections = page_soup.select('div.mta-table')

    for section in table_sections:
        # Get the title of the table section
        title_element = section.find('h2')
        if title_element:
            title = title_element.text.strip()
            stations = {}

            # Find all rows in the table body
            rows = section.select('tbody tr')
            for row in rows:
                # The station name is in the first cell (td) of the row
                station_cell = row.find('td')
                if station_cell:
                    # Clean up the text to remove hidden characters
                    station_name = station_cell.text.strip()
                    if station_name:
                        gtfs_stop_id = stations_df[stations_df['Stop Name'] == station_name]['GTFS Stop ID'].values
                        if not gtfs_stop_id.size > 0:
                            # Try partial match if exact match not found
                            gtfs_stop_id = stations_df[stations_df['Stop Name'].str.contains(station_name, na=False)]['GTFS Stop ID'].values
                        if not gtfs_stop_id.size > 0:
                            # Apply all transformations to a copy of the name
                            alt_name = station_name
                            alt_name = alt_name.replace('Square', 'Sq')
                            alt_name = alt_name.replace('Parkway', 'Pkwy')
                            alt_name = alt_name.replace('Heights', 'Hts')
                            alt_name = alt_name.replace('Ave', 'Av')
                            if 'Pkwy' not in alt_name:
                                alt_name = alt_name.replace('Pk', 'Park')
                            if 'Northern' not in alt_name:
                                alt_name = alt_name.replace('North', 'N')
                            if 'Southern' not in alt_name:
                                alt_name = alt_name.replace('South', 'S')
                            if 'Western' not in alt_name and 'Westchester' not in alt_name and 'West Farms' not in alt_name:
                                alt_name = alt_name.replace('West', 'W')
                            if 'Eastern' not in alt_name and not alt_name.endswith(' East'):
                                alt_name = alt_name.replace('East', 'E')
                            alt_name = alt_name.replace("E 143 St-Mary's St", "E 143 St-St Mary's St")
                            alt_name = alt_name.replace('Bleeker St', 'Bleecker St') # Possible typo
                            alt_name = alt_name.replace('St/Port', 'St-Port')
                            alt_name = alt_name.replace('Washington Sq', 'Wash Sq')
                            alt_name = alt_name.replace('Boulevard', 'Blvd')
                            alt_name = alt_name.replace('Bryant Park', 'Bryant Pk')
                            alt_name = alt_name.replace('4-', '4 St-')
                            alt_name = alt_name.replace('Bay-50', 'Bay 50')
                            alt_name = alt_name.replace('Sts Rockefeller', 'Sts-Rockefeller')
                            alt_name = alt_name.replace('Myrtle Willoughby', 'Myrtle-Willoughby')
                            alt_name = alt_name.replace('4 Av-9 Sts', '4 Av-9 St')
                            alt_name = alt_name.replace('Astoria Ditmars Blvd', 'Astoria-Ditmars Blvd')
                            alt_name = alt_name.replace('57-7 Av', '57 St-7 Av')
                            alt_name = alt_name.replace('Delancey St Essex St', 'Delancey St-Essex St')
                            
                            # Search with the fully transformed name
                            gtfs_stop_id = stations_df[stations_df['Stop Name'] == alt_name]['GTFS Stop ID'].values

                        if gtfs_stop_id.size > 0:
                            stations[gtfs_stop_id[0]] = station_name
                        else:
                            raise ValueError(f"Station '{station_name}' / '{alt_name}' not found in CSV data.")

            if title and stations:
                all_lines_data[train_line][title] = stations

# Hardcode data for the Staten Island Railway (SI)
si_stations_ordered = {
    "S09": "Tottenville",
    "S11": "Arthur Kill",
    "S13": "Richmond Valley",
    "S14": "Pleasant Plains",
    "S15": "Prince's Bay",
    "S16": "Huguenot",
    "S17": "Annadale",
    "S18": "Eltingville",
    "S19": "Great Kills",
    "S20": "Bay Terrace",
    "S21": "Oakwood Heights",
    "S22": "New Dorp",
    "S23": "Grant City",
    "S24": "Jefferson Av",
    "S25": "Dongan Hills",
    "S26": "Old Town",
    "S27": "Grasmere",
    "S28": "Clifton",
    "S29": "Stapleton",
    "S30": "Tompkinsville",
    "S31": "St George",
}

# Reverse the dictionary order as requested
reversed_si_stations = dict(reversed(list(si_stations_ordered.items())))

# Add the SI line data to the main dictionary
all_lines_data["SI"] = {
    "Staten Island Railway": reversed_si_stations
}

print("\n--- All data collected ---")
# Pretty-print the final dictionary
# print(json.dumps(all_lines_data, indent=2))
# Save the data to a JSON file
with open("mta_subway_stations.json", "w") as json_file:
    json.dump(all_lines_data, json_file, indent=2)
print("Data saved to mta_subway_stations.json")