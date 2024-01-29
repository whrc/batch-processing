import os
from abc import ABC, abstractmethod


class BaseCommand(ABC):
    def __init__(self):
        self.home_dir = os.getenv("HOME")
        self.user = os.getenv("USER")

    @abstractmethod
    def execute(self):
        pass
