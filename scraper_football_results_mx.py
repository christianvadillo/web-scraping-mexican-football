import scrapy
import json

from collections import defaultdict
from datetime import datetime
from pathlib import Path
from pprint import pprint
from scrapy.crawler import CrawlerProcess
from scrapy.selector import SelectorList
from typing import Dict, List, Optional

from data_entities import Game
from db import GamesSQlite


db_games = GamesSQlite()


def _filter_links(links: List[str]) -> List[str]:
    """Get the links of the games after the 2002 seasons.
        # The data is correctly structured after the 2002 season
        # The last 4 characters of the url are the season year

    Args:
        links (List[str]): links of the seasons

    Returns:
        List[str]: Links of the games after the 2002 seasons
    """

    filtered = [(int(link[-4:]), link) for link in links if not link.endswith("mexico")]
    filtered = [link[1] for link in filtered if link[0] > 2002]
    filtered += [link for link in links if link.endswith("mexico")]
    return filtered


class ResultsSpider(scrapy.Spider):
    """Scrape the results of the mx soccer league from the resultados-futbol.com website"""

    name = "test"
    custom_settings = {
        "COOKIES_ENABLED": False,
        "DOWNLOAD_DELAY": 5,
    }
    seasons: Dict[str, List[Game]] = defaultdict(list)

    def start_requests(self):
        urls = [
            "https://www.resultados-futbol.com/apertura_mexico",
            # "https://www.resultados-futbol.com/clausura_mexico",
            # "https://www.resultados-futbol.com/etapas_finales_apertura_mexico",
        ]
        for url in urls:
            print(url)
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        season_links = response.css(
            "div#desplega_temporadas > ul > li > a::attr(href)"
        ).extract()

        season_links = _filter_links(season_links)
        for season_url in season_links:
            yield response.follow(url=season_url, callback=self.parse_season)

    def parse_season(self, response):
        matchdays_links = response.css(
            "div#desplega_jornadas > ul > li > a::attr(href)"
        ).extract()

        for url in matchdays_links:
            yield response.follow(url=url, callback=self.parse_matchdays)

    def parse_matchdays(self, response):
        results_table_block = response.css("div.contentitem")
        category = results_table_block.css(
            "#category_alias::attr(value)"
        ).extract_first()
        year = results_table_block.css("#year::attr(value)").extract_first()

        self.active_season = category + "_" + year
        self.jornada = response.css("div.j_cur > a::text").extract()
        self._extract_matchday_data(results_table_block)

    def _extract_matchday_data(self, block: SelectorList) -> Game:
        results_table = block.css("table#tabla1")
        for row in results_table.css("tr.vevent"):
            result = row.css("td.rstd > a.url > span.clase::text").extract_first()
            try:
                game = Game(
                    datetime=row.css("td.fecha::attr(title)").extract_first(),
                    home_team=row.css("td.equipo1 > a::text").extract_first(),
                    away_team=row.css("td.equipo2> a::text").extract_first(),
                    home_goal=result.split("-")[0],
                    away_goal=result.split("-")[1],
                    matchday=self.jornada[-1],
                )
                ResultsSpider.seasons[self.active_season].append(game)
                db_games.insert(game)
            except AttributeError as e:
                print(e)
                import pdb

                pdb.set_trace()
        print("------------------")
        print(f"Temporada {self.active_season} - Jornada {self.jornada[-1]}")
        print(f"Scrapped {len(ResultsSpider.seasons[self.active_season])} games")
        print("------------------")


def to_json(results: ResultsSpider):
    results_dir = Path("results")
    results_dir.mkdir(exist_ok=True)

    for season, games in results.seasons.items():
        with open(results_dir / f"{season}.txt", "w", encoding="utf-8") as f:
            for game in games:
                game.datetime = datetime.strftime(game.datetime, "%Y-%m-%d %H:%M")
                json.dump(game.dict(), f)
                f.write(",\n")


def scrape_page():
    # Run the Spider
    process = CrawlerProcess()
    process.crawl(ResultsSpider)
    process.start()

    print(ResultsSpider.seasons.keys())
    to_json(ResultsSpider)

    for season in ResultsSpider.seasons:
        print(len(ResultsSpider.seasons[season]))


if __name__ == "__main__":
    scrape_page()
