import requests
import asyncio
import aiohttp
import sqlite3
import re
import time
import os


BASE_URL = "https://eu1.offering-api.kambicdn.com/offering/v2018/"

def create_database(offering):
    db_file = f"events_{offering}.db"
    if os.path.exists(db_file):
        os.remove(db_file)
        print(f"{db_file} deleted.")
    
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY,
            sport TEXT,
            home_name TEXT,
            away_name TEXT,
            start TEXT,
            event_group TEXT,
            odds_1 REAL,
            odds_x REAL,
            odds_2 REAL
        )
    """)
    conn.commit()
    print(f"{db_file} created.")
    return conn

def insert_bulk_events(cursor, events):
    cursor.executemany("""
        INSERT OR IGNORE INTO events (id, sport, home_name, away_name, start, event_group, odds_1, odds_x, odds_2)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, events)

def clean_team_name(name):
    cleaned_name = re.sub(r'\(\d+\)', '', name).strip()
    return cleaned_name

def process_json_and_store(data, cursor, sport):
    events_to_insert = []
    for event in data:
        event_info = event.get("event", {})
        if event_info.get("state") == "NOT_STARTED":
            home_name = clean_team_name(event_info.get("homeName"))
            away_name = clean_team_name(event_info.get("awayName"))
            start = event_info.get("start")
            event_group = event_info.get("group")

            bet_offers = event.get("betOffers", [])
            odds_1, odds_x, odds_2 = None, None, None

            for offer in bet_offers:
                if offer.get("betOfferType", {}).get("id") == 2:
                    outcomes = offer.get("outcomes", [])
                    for outcome in outcomes:
                        type = outcome.get("type")
                        odds = outcome.get("odds")
                        if odds is not None:
                            if type == "OT_ONE":
                                odds_1 = odds / 1000.0
                            elif type == "OT_CROSS":
                                odds_x = odds / 1000.0
                            elif type == "OT_TWO":
                                odds_2 = odds / 1000.0
                    break

            event_data = (
                event_info.get("id"),
                sport,
                home_name,
                away_name,
                start,
                event_group,
                odds_1,
                odds_x,
                odds_2
            )
            events_to_insert.append(event_data)

    insert_bulk_events(cursor, events_to_insert)

async def fetch_sports_events(sport, offering):
    def collect_paths(data, urls=[]):
        if isinstance(data, list):
            for item in data:
                collect_paths(item, urls)
        elif isinstance(data, dict):
            if data.get("level") == 3.0 and "path" in data and data["path"]:
                parts = data["path"].split("/")
                if len(parts) == 3:
                    full_path = f"{data['path']}/all"
                elif len(parts) == 2:
                    full_path = f"{data['path']}/all/all"
                else:
                    full_path = data["path"]
                urls.append(f"{BASE_URL}{offering}/listView/{full_path}/matches.json?lang=lv_LV&market=LV&useCombined=true")
            for key, value in data.items():
                if isinstance(value, (list, dict)):
                    collect_paths(value, urls)
        return urls

    graphql_url = "https://graphql.kambicdn.com/"
    payload = {
        "query": """
            query getGroups($sport: String!, $offering: String!, $market: String!, $language: String!) {
                groups(
                    sport: $sport
                    groupInternationalGroups: true
                    addAllSubGroupsToTopLeagues: true
                    offering: $offering
                    market: $market
                    language: $language
                ) {
                    groups {
                        name
                        level
                        id
                        countryCode
                        abbreviation
                        path
                        groups {
                            name
                            id
                            level
                            path
                        }
                    }
                    topLeagues {
                        id
                        name
                        sortOrder
                        countryCode
                        path
                    }
                }
            }
        """,
        "variables": {
            "sport": sport,
            "offering": offering,
            "language": "lv_LV",
            "market": "LV"
        }
    }
    headers = {
        "Content-Type": "application/json",
        "Accept": "*/*",
        "authorization": "kambi"
    }

    response = requests.post(graphql_url, json=payload, headers=headers)
    if response.status_code != 200:
        print(f"Failed to fetch paths: {response.status_code}")
        return []

    data = response.json()
    urls = collect_paths(data)

    async def fetch_events(session, url):
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("events", [])
                else:
                    print(f"Failed to fetch {url}: {response.status}")
                    return []
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            return []

    async def fetch_all_events(urls):
        events = []
        connector = aiohttp.TCPConnector(limit=50) 
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [fetch_events(session, url) for url in urls]
            results = await asyncio.gather(*tasks)
            for index, result in enumerate(results):
                events.extend(result)
        return events
    
    all_events = await fetch_all_events(urls)
    return all_events

async def main(offering):
    url = f"{BASE_URL}{offering}/group.json?lang=lv_LV&market=LV"
    response = requests.get(url)
    data = response.json()

    sports = []
    if "group" in data and "groups" in data["group"]:
        for group in data["group"]["groups"]:
            if "termKey" in group:
                sports.append(group["termKey"])

    conn = create_database(offering)
    cursor = conn.cursor()

    start_time = time.time()

    for sport in sports:
        all_events = await fetch_sports_events(sport, offering)

        process_json_and_store(all_events, cursor, sport)

    conn.commit()
    conn.close()
    print(f'Data from "{offering}" successfully stored in events_{offering}.db')

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Total time: {elapsed_time} seconds")

if __name__ == "__main__":
    asyncio.run(main('paflv'))