"""
CS 457 Final Project
models.py
Data classes for Steam Explorer
"""

from dataclasses import dataclass, field
from datetime import date

@dataclass
class Publisher:
    publisher_id: int
    name: str

@dataclass
class Developer:
    developer_id: int
    name: str

@dataclass
class Genre:
    genre_id: int
    name: str

@dataclass
class Category:
    category_id: int
    name: str

@dataclass
class Review:
    appid: int
    positive_ratings: int
    negative_ratings: int

    @property
    def total(self) -> int:
        return self.positive_ratings + self.negative_ratings
    
    @property
    def score(self) -> float:
        if self.total == 0:
            return 0.0
        return round(self.positive_ratings / self.total * 100 ,1)

@dataclass
class PlatformSupport:
    appid: int
    windows: bool
    mac: bool
    linux: bool

@dataclass
class Game:
    appid: int
    name: str
    developer: str | None
    publisher: str | None
    release_date: date | None
    price: float | None
    average_playtime: int
    owners: str | None
    genres: list[str] = field(default_factory=list)
    categories: list[str] = field(default_factory=list)
    platforms: PlatformSupport | None = None

    @property
    def release_year(self) -> int | None:
        return self.release_date.year if self.release_date else None
    @property
    def display_price(self) -> str:
        if self.price is None:
            return "N/A"
        return "Free" if self.price == 0 else f"${self.price:.2f}"
    

