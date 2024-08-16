import toml
import requests
import pandas as pd
from sqlalchemy.exc import OperationalError, ProgrammingError

with open("config.toml", "r") as f:
	config = toml.load(f)

CONNECTION_STRING = config["database"]["dev_db_connection_string"]


def call_api(type_id):
	url = f"https://pokeapi.co/api/v2/type/{type_id}"
	r = requests.get(url)

	type_id = r.json()["id"]
	name = r.json()["name"]

	double_damage_to_array = []
	for i in r.json()["damage_relations"]["double_damage_to"]:
		double_damage_to_array.append(i["name"])

	return type_id, name, double_damage_to_array


def build_dataframe(start: int = 1, end: int = 19) -> pd.DataFrame:
	data = []
	for i in range(start, end):
		type_id, name, double_damage_to_array = call_api(i)

		data.append(
			{
				"type_id": type_id,
				"name": name,
				"double_damage_to": double_damage_to_array,
			}
		)

	types_df = pd.DataFrame(data)

	return types_df


def upload_dataframe(types_df: pd.DataFrame):

	try:
		types_df.to_sql(
			name = "types",
			con = CONNECTION_STRING,
			schema = "pokemon-schema",
			if_exists = "replace",
			index=False
		)
	except OperationalError as e:
		print(e)
	except ProgrammingError as e:
		print(e)


if __name__ == "__main__":
	df = build_dataframe()
	upload_dataframe(df)
