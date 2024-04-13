
import numpy as np
from sklearn import neighbors
import pika
import pandas as pd
import psycopg2 as pg
import json

with open("secrets.txt") as f:
    lines = f.readlines()
    connection_string = lines[0].strip()
    pika_username = lines[1].strip()
    pika_password = lines[2].strip()


EXCHANGE_NAME = 'recommendations'

def get_games_of_player(cursor) -> np.ndarray:
    cursor.execute('select * from games_players order by id_player')
    result_query = np.array(cursor.fetchall())
    return result_query


def get_neighbours(raw_data, to_predict):
    data = pd.DataFrame(raw_data,
                        columns=['id', 'genre', 'is_multiplayer', 'rating'])
    data = pd.concat([data, to_predict], ignore_index=True)
    data_cutted = data.drop(labels=['id'], axis=1)
    imaginary_rows = len(to_predict)

    data_cutted = pd.get_dummies(data_cutted, columns=['genre'])

    ng = neighbors.NearestNeighbors(n_neighbors=10)

    ng.fit(data_cutted[:-imaginary_rows])
    distances, indices = ng.kneighbors(data_cutted[-imaginary_rows:])

    return data['id'][indices[0]].values


rabbit_connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672,
                                                  credentials=pika.credentials.PlainCredentials(username=pika_username, password=pika_password)))
rabbit_channel = rabbit_connection.channel()
connection = pg.connect(connection_string)
cursor = connection.cursor()

cursor.execute('select * from games_players')
players_games_res = cursor.fetchall()

player_game_dict = {}
for player_id, game_id in players_games_res:
    player_game_dict[player_id] = player_game_dict.get(player_id, []) + [game_id]

for player_id, game_ids in player_game_dict.items():
    games = str(game_ids).removeprefix('[').removesuffix(']')

    cursor.execute(
        query=f'select id, genre, is_multiplayer, rating from games TABLESAMPLE bernoulli(round(1000.0 / (select COUNT(id) from games) * 100, 0)) WHERE id NOT IN ({games})')
    resultGames = cursor.fetchall()

    cursor.execute(f'SELECT id, genre, is_multiplayer, rating FROM games WHERE id IN ({games})')
    resultPlayers = cursor.fetchall()

    recommended_games = get_neighbours(resultGames, pd.DataFrame(resultPlayers, columns=['id', 'genre', 'is_multiplayer', 'rating']))
    resultDictionary = {player_id: recommended_games.tolist()}
    json_result = json.dumps(resultDictionary)
    rabbit_channel.basic_publish(exchange=EXCHANGE_NAME, routing_key='', body=json_result)


rabbit_connection.close()
cursor.close()
connection.close()
