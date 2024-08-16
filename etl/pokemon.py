import toml
import requests
import pandas as pd
from sqlalchemy.exc import OperationalError, ProgrammingError

with open("config.toml", "r") as f:
    config = toml.load(f)

CONNECTION_STRING = config["database"]["dev_db_connection_string"]


def call_api(pokemon_id) -> tuple[int, str, str, str, list[str], list[str], list[str]]:
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
        "hidden_ability_url": None
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
        [abilities["ability_1_url"], abilities["ability_2_url"], abilities["hidden_ability_url"]]
    )


def build_dataframe(start: int = 1, end: int = 1026) -> pd.DataFrame:
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
    dataframe = build_dataframe()

    try:
        pokemon_df.to_sql(
            name="pokemon",
            con=CONNECTION_STRING,
            schema="pokemon-schema",
            if_exists="replace",
            index=False
        )
    except OperationalError as e:
        print(e)
    except ProgrammingError as e:
        print(e)


if __name__ == "__main__":
    df = build_dataframe()
    upload_dataframe(df)
