"""
src/data_fetcher.py
Fetches raw data for Plan A (Understat with xG, 2014-present)
and Plan B (Football-Data.co.uk without xG, 2000-present).
"""

import asyncio  # Built-in library for running asynchronous concurrent operations
import os       # Operating system interface for managing paths and directories
import pandas as pd  # Data manipulation library for tabular data processing
from understatapi import UnderstatClient  # Python client wrapper to extract Understat data

# Construct relative file paths for raw and processed data storage
RAW_DATA_DIR = os.path.join("data", "raw")  # Evaluates to "data/raw" (cross-platform compatible)
PROCESSED_DATA_DIR = os.path.join("data", "processed")  # Evaluates to "data/processed"

# Automatically create the directories on your computer if they do not exist already
os.makedirs(RAW_DATA_DIR, exist_ok=True)  # exist_ok=True prevents errors if folder already exists
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)  # Creates data/processed directory

# List of Understat league identifiers for the top 5 European leagues
TOP_5_LEAGUES_UNDERSTAT = ["EPL", "La_liga", "Bundesliga", "Serie_A", "Ligue_1"]

# Generate list of start years: 2014 to 2025 (representing 2014/15 through 2025/26 seasons)
UNDERSTAT_SEASONS = list(range(2014, 2026))

# Map Football-Data.co.uk internal league codes to clean league names
FOOTBALL_DATA_LEAGUES = {
    "E0": "EPL",        # English Premier League
    "SP1": "La_Liga",   # Spanish La Liga
    "I1": "Serie_A",    # Italian Serie A
    "D1": "Bundesliga", # German Bundesliga
    "F1": "Ligue_1",    # French Ligue 1
}


# ==========================================
# PLAN A: UNDERSTAT (xG Included)
# ==========================================

async def fetch_understat_league_season(client: UnderstatClient, league: str, season: int) -> pd.DataFrame:
    """
    Asynchronously fetches team-level aggregated statistics for a single league and season.
    Parameters:
      - client: Active UnderstatClient connection session
      - league: League identifier (e.g. 'EPL')
      - season: Starting year of season (e.g. 2021 for 2021/22)
    """
    try:
        # Request raw dictionary payload for specified league and season from Understat
        data = client.league(league=league).get_team_data(season=str(season))
        
        records = []  # List to accumulate dictionary records for each team in the league
        
        # Iterate through each team in the returned JSON structure (team_id is key, team_info is value)
        for team_id, team_info in data.items():
            title = team_info["title"]      # Extract official team name (e.g., 'Manchester City')
            history = team_info["history"]  # List of match dictionaries containing performance data
            
            # Compute team-season aggregates from individual match history
            matches = len(history)  # Total matches played in that season
            won = sum(1 for m in history if m["result"] == "w")   # Count total wins
            drawn = sum(1 for m in history if m["result"] == "d") # Count total draws
            lost = sum(1 for m in history if m["result"] == "l")  # Count total losses
            pts = sum(m["pts"] for m in history)                   # Sum total league points accrued
            goals_for = sum(m["scored"] for m in history)         # Sum total goals scored (Attack)
            goals_against = sum(m["missed"] for m in history)     # Sum total goals conceded (Defence)
            xg_for = sum(m["xG"] for m in history)                 # Sum total Expected Goals (xG)
            xg_against = sum(m["xGA"] for m in history)            # Sum total Expected Goals Against (xGA)
            
            # Append aggregated team-season record as a dictionary to our list
            records.append({
                "league": league,
                "season": f"{season}/{season+1}",
                "team": title,
                "matches": matches,
                "wins": won,
                "draws": drawn,
                "losses": lost,
                "points": pts,
                "goals_scored": goals_for,
                "goals_conceded": goals_against,
                "xg": xg_for,
                "xga": xg_against,
            })
            
        # Convert list of dictionaries into a clean Pandas DataFrame and return it
        return pd.DataFrame(records)
        
    except Exception as e:
        # Catch and report any network or parsing errors without crashing the pipeline
        print(f"Error fetching Understat data for {league} {season}: {e}")
        return pd.DataFrame()  # Return an empty DataFrame if request failed


async def run_understat_pipeline():
    """Main asynchronous orchestrator for collecting all Plan A (Understat) data."""
    print("--- Starting Plan A: Understat Data Retrieval ---")
    all_frames = []  # Holds DataFrames returned for each league-season pair
    
    # Initialize the Understat client context manager
    with UnderstatClient() as client:
        for league in TOP_5_LEAGUES_UNDERSTAT:      # Loop over all 5 leagues
            for season in UNDERSTAT_SEASONS:         # Loop over all 12 seasons
                print(f"Fetching Understat: {league} ({season}/{season+1})...")
                
                # Await asynchronous fetch for the specific league and season
                df = await fetch_understat_league_season(client, league, season)
                
                if not df.empty:
                    all_frames.append(df)  # Store non-empty DataFrame
                
                await asyncio.sleep(0.3)  # Pause 0.3s to respect rate limits and avoid blocking
                
    # If at least one league-season was successfully fetched
    if all_frames:
        combined_df = pd.concat(all_frames, ignore_index=True)  # Merge into single DataFrame
        out_path = os.path.join(RAW_DATA_DIR, "plan_a_understat_raw.csv")  # Target filepath
        combined_df.to_csv(out_path, index=False)  # Save DataFrame to disk as CSV
        print(f"\nSuccessfully saved Plan A raw data to: {out_path} ({len(combined_df)} records)")
    else:
        print("Failed to pull Plan A data.")


# ==========================================
# PLAN B: FOOTBALL-DATA.CO.UK (Historical 2000+)
# ==========================================

def fetch_football_data_co_uk(start_year: int = 2000, end_year: int = 2025) -> pd.DataFrame:
    """Downloads match statistics CSVs directly from Football-Data.co.uk."""
    print("\n--- Starting Plan B: Football-Data.co.uk Retrieval ---")
    match_list = []  # List to accumulate raw match DataFrames
    
    for year in range(start_year, end_year + 1):  # Loop through years 2000 to 2025
        season_str = f"{str(year)[-2:]}{str(year+1)[-2:]}"  # Formats year e.g. 2004 -> "0405"
        season_label = f"{year}/{year+1}"                   # Formats label e.g. "2004/2005"
        
        for code, league_name in FOOTBALL_DATA_LEAGUES.items():  # Loop over league codes
            url = f"https://www.football-data.co.uk/mmz4281/{season_str}/{code}.csv"
            try:
                # Read CSV directly from web URL into Pandas
                df = pd.read_csv(url, encoding="unicode_escape")
                
                req_cols = ["HomeTeam", "AwayTeam", "FTHG", "FTAG"]  # Minimum required match columns
                if not all(col in df.columns for col in req_cols):
                    continue  # Skip invalid or incomplete CSV files
                
                df = df.dropna(subset=["HomeTeam", "AwayTeam"])  # Drop empty rows
                df["League"] = league_name                       # Add league label column
                df["Season"] = season_label                       # Add season label column
                match_list.append(df)                            # Append match DataFrame to list
            except Exception:
                continue  # Skip files that raise 404 or formatting errors
                
    if match_list:
        raw_matches = pd.concat(match_list, ignore_index=True)  # Combine all match tables
        out_path = os.path.join(RAW_DATA_DIR, "plan_b_matches_raw.csv")  # Target filepath
        raw_matches.to_csv(out_path, index=False)  # Save raw match data to disk
        print(f"Successfully saved Plan B match data to: {out_path} ({len(raw_matches)} matches)")
        return raw_matches
    else:
        print("Failed to download Football-Data.co.uk data.")
        return pd.DataFrame()


# Standard Python entrypoint check
if __name__ == "__main__":
    # Execute Plan A pipeline inside an asyncio event loop
    asyncio.run(run_understat_pipeline())
    
    # Execute Plan B historical pipeline synchronously
    fetch_football_data_co_uk(start_year=2000, end_year=2025)