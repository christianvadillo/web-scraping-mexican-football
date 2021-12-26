import locale
import scrapy

from collections import defaultdict
from datetime import datetime
from pprint import pprint
from pydantic import BaseModel, validator, ValidationError
from scrapy.crawler import CrawlerProcess
from scrapy.selector import SelectorList
from typing import Dict, List, Optional


locale.setlocale(locale.LC_ALL, "es_ES")  # To parse correctly the months


class Game(BaseModel):
    datetime: datetime
    home_team: str
    away_team: str
    home_goal: Optional[int]
    away_goal: Optional[int]
    matchday: int

    @validator("matchday", pre=True)
    def validate_matchday(cls, value):
        if value is None:
            raise ValidationError("matchday is required")
        return value.strip(" ")[-1]

    @validator("datetime", pre=True)
    def validate_datetime(cls, value):
        if value is None:
            raise ValidationError("datetime is required")
        try:
            return datetime.strptime(value, "%d %b %y a las %H:%M")
        except Exception as e:
            print(e)
            import pdb

            pdb.set_trace()


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
    filtered = [link[1] for link in filtered if link[0] > 2017]
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
                ResultsSpider.seasons[self.active_season].append(
                    Game(
                        datetime=row.css("td.fecha::attr(title)").extract_first(),
                        home_team=row.css("td.equipo1 > a::text").extract_first(),
                        away_team=row.css("td.equipo2> a::text").extract_first(),
                        home_goal=result.split("-")[0],
                        away_goal=result.split("-")[1],
                        matchday=self.jornada[-1],
                    )
                )
            except AttributeError as e:
                print(e)
                import pdb

                pdb.set_trace()
        print("------------------")
        print(f"Temporada {self.active_season} - Jornada {self.jornada[-1]}")
        print(f"Scrapped {len(ResultsSpider.seasons[self.active_season])} games")
        print("------------------")


def scrape_page():
    # Run the Spider
    process = CrawlerProcess()
    process.crawl(ResultsSpider)
    process.start()

    print(ResultsSpider.seasons.keys())
    for season in ResultsSpider.seasons:
        print(len(ResultsSpider.seasons[season]))
    import pdb

    pdb.set_trace()

    # for g in ResultsSpider.seasons["apertura_mexico_2022"]:
    #     pprint(g.__dict__)


if __name__ == "__main__":
    scrape_page()
