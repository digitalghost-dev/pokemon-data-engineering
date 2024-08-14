import toml
import requests
import polars as pl
from sqlalchemy.exc import OperationalError, ProgrammingError

with open("config.toml", "r") as f:
    config = toml.load(f)

CONNECTION_STRING = config["database"]["connection_string"]


def call_api(pokemon_id) -> tuple[int, str, str, str, list[str], list[str]]:
    url = f"https://pokeapi.co/api/v2/pokemon/{pokemon_id}"
    r = requests.get(url)

    pokemon_data = r.json()

    pokemon_id = pokemon_data["id"]
    name = pokemon_data["name"]
    height = pokemon_data["height"]
    weight = pokemon_data["weight"]

    types = [t["type"]["name"] for t in pokemon_data["types"]]

    ability_1 = None
    ability_2 = None
    hidden_ability = None

    for ability in pokemon_data["abilities"]:
        ability_name = ability["ability"]["name"]
        if ability["is_hidden"]:
            hidden_ability = ability_name
        elif ability["slot"] == 1:
            ability_1 = ability_name
        elif ability["slot"] == 2:
            ability_2 = ability_name

    return pokemon_id, height, weight, name, types, [ability_1, ability_2, hidden_ability]


def build_dataframe() -> pl.DataFrame:
    data = []
    # Current number of Pokémon is 1025
    for i in range(1, 1026):
        pokemon_id, height, weight, name, types, abilities = call_api(i)

        types += [None] * (2 - len(types))
        abilities += [None] * (3 - len(abilities))

        data.append(
            {
                "pokemon_id": pokemon_id,
                "height": height,
                "weight": weight,
                "name": name,
                "type_1": types[0],
                "type_2": types[1],
                "ability_1": abilities[0],
                "ability_2": abilities[1],
                "hidden_ability": abilities[2],
            }
        )

    df = pl.DataFrame(data)

    with pl.Config(tbl_cols=-1, tbl_rows=-1, fmt_str_lengths=100, tbl_width_chars=1000):
        # print(df)
        return df


def upload_dataframe():
    dataframe = build_dataframe()

    try:
        dataframe.write_database(
            table_name="pokemon-schema.pokemon",
            connection=CONNECTION_STRING,
            if_table_exists="replace",
        )
    except OperationalError as e:
        print(e)
    except ProgrammingError as e:
        print(e)


if __name__ == "__main__":
    upload_dataframe()
