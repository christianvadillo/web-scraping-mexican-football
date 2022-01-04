import sqlite3
import json

from pathlib import Path
from typing import List

from data_entities import Game


class GamesSQlite:
    def __init__(self):
        self.conn = sqlite3.connect("football_results.db")
        self.c = self.conn.cursor()
        self.create_table()

    def create_table(self) -> None:
        with self.conn:
            self.c.execute(
                """CREATE TABLE IF NOT EXISTS games (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                home_team TEXT,
                away_team TEXT,
                home_goal INTEGER,
                away_goal INTEGER,
                matchday INTEGER
                )"""
            )

    def insert(self, game: Game) -> None:
        with self.conn:
            self.c.execute(
                """INSERT INTO games (date, home_team, away_team, home_goal, away_goal, matchday)
                VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    game.datetime.strftime("%Y-%m-%d %H:%M"),
                    game.home_team,
                    game.away_team,
                    game.home_goal,
                    game.away_goal,
                    game.matchday,
                ),
            )

    @property
    def games(self) -> List[str]:
        self.c.execute("SELECT * FROM games")
        return self.c.fetchall()


# def load_data():
#     results_dir_path = Path("results")
#     files = list(results_dir_path.glob("*.txt"))

#     for file in files:
#         with open(file, "r", encoding="utf-8") as f:
#             records = f.readlines()
#             for record in records:
#                 game = Game(**json.loads(record.replace(",\n", "")))
#                 import pdb

#                 pdb.set_trace()


# if __name__ == "__main__":
#     # load_data()
#     db_games = GamesSQlite()
