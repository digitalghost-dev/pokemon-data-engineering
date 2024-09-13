import toml
import os
import requests
import pandas as pd
from sqlalchemy.exc import OperationalError, ProgrammingError

current_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(current_dir, "config.toml")

with open(config_path, "r") as f:
    config = toml.load(f)


CONNECTION_STRING = config["database"]["dev_db_connection_string"]
pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", 500)
pd.set_option("display.max_colwidth", None)


def call_api(pokemon_id) -> tuple[int, str, str, str, list[str], list[str], list[str]]:
    """
    Fetches data for a specific Pokémon from the PokeAPI.

    Args:
        pokemon_id (int): The ID of the Pokémon to fetch.

    Returns:
        tuple: A tuple containing the Pokémon's ID, height, weight, name, types, abilities, and ability URLs.
    """
    url = f"https://pokeapi.co/api/v2/pokemon/{pokemon_id}"
    r = requests.get(url)

    pokemon_data = r.json()

    pokemon_id = pokemon_data["id"]
    name = pokemon_data["name"]
    height = pokemon_data["height"]
    weight = pokemon_data["weight"]

    types = [t["type"]["name"] for t in pokemon_data["types"]]

    abilities = {
        "ability_1": None,
        "ability_2": None,
        "hidden_ability": None,
        "ability_1_url": None,
        "ability_2_url": None,
        "hidden_ability_url": None,
    }

    for ability in pokemon_data["abilities"]:
        ability_name = ability["ability"]["name"]
        ability_url = ability["ability"]["url"]
        if ability["is_hidden"]:
            abilities["hidden_ability"] = ability_name
            abilities["hidden_ability_url"] = ability_url
        elif ability["slot"] == 1:
            abilities["ability_1"] = ability_name
            abilities["ability_1_url"] = ability_url
        elif ability["slot"] == 2:
            abilities["ability_2"] = ability_name
            abilities["ability_2_url"] = ability_url

    return (
        pokemon_id,
        height,
        weight,
        name,
        types,
        [abilities["ability_1"], abilities["ability_2"], abilities["hidden_ability"]],
        [abilities["ability_1_url"], abilities["ability_2_url"], abilities["hidden_ability_url"]],
    )


def build_dataframe(start: int = 1, end: int = 1026) -> pd.DataFrame:
    """
    Builds a pandas DataFrame containing Pokémon data for a range of Pokémon IDs.

    Args:
        start (int, optional): The starting Pokémon ID. Defaults to 1.
        end (int, optional): The ending Pokémon ID. Defaults to 1026.

    Returns:
        pd.DataFrame: A DataFrame containing Pokémon data including ID, height, weight, name, types, and abilities.
    """
    data = []
    # Current number of Pokémon is 1025
    for i in range(start, end):
        pokemon_id, height, weight, name, types, ability_name, ability_url = call_api(i)

        types += [None] * (2 - len(types))
        ability_name += [None] * (3 - len(ability_name))
        ability_url += [None] * (3 - len(ability_url))

        data.append(
            {
                "pokemon_id": pokemon_id,
                "height": height,
                "weight": weight,
                "name": name,
                "type_1": types[0],
                "type_2": types[1],
                "ability_1": ability_name[0],
                "ability_1_url": ability_url[0],
                "ability_2": ability_name[1],
                "ability_2_url": ability_url[1],
                "hidden_ability": ability_name[2],
                "hidden_ability_url": ability_url[2],
            }
        )

    pokemon_df = pd.DataFrame(data)

    return pokemon_df


def upload_dataframe(pokemon_df: pd.DataFrame):
    """
    Uploads a pandas DataFrame containing Pokémon data to a database.

    Args:
        pokemon_df (pd.DataFrame): The DataFrame containing Pokémon data to be uploaded.

    Raises:
        OperationalError: If there is an operational error during the upload process.
        ProgrammingError: If there is a programming error during the upload process.
    """
    dataframe = build_dataframe()

    try:
        pokemon_df.to_sql(
            name="pokemon", con=CONNECTION_STRING, schema="pokemon-schema", if_exists="replace", index=False
        )
    except OperationalError as e:
        print(e)
    except ProgrammingError as e:
        print(e)


def call_dataframe():
    """
    Retrieves Pokémon data from the database, adds image URLs, and uploads the updated DataFrame back to the database.

    Raises:
        pd.errors.EmptyDataError: If there is an empty data error during the data retrieval process.
        OperationalError: If there is an operational error during the data retrieval process.
    """
    main_df = build_dataframe()
    try:
        pokemon_names_df = pd.read_sql_query(
            """            
            SELECT name
            FROM "pokemon-schema"."pokemon"
            ORDER BY "pokemon_id"
            """,
            CONNECTION_STRING,
        )
        name_list = list(pokemon_names_df["name"])
        base_url = "https://pokemon-objects.nyc3.digitaloceanspaces.com/"
        image_url_list = [base_url + name + ".png" for name in name_list]

        images_df = pd.DataFrame({"image_url": image_url_list})
        concat_df = pd.concat([main_df, images_df], axis=1)
        concat_df.to_sql(
            name="pokemon", con=CONNECTION_STRING, schema="pokemon-schema", if_exists="replace", index=False
        )
    except pd.errors.EmptyDataError as e:
        print(e)
    except OperationalError as e:
        print(e)


if __name__ == "__main__":
    df = build_dataframe()
    upload_dataframe(df)
    call_dataframe()
