import datetime
import logging

from common.opeartor import StatefulFunction

logging.Formatter.formatTime = (lambda self, record, datefmt: datetime.datetime.
                                fromtimestamp(record.created, datetime.timezone.utc).astimezone().isoformat())

logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s',
                    level=logging.INFO)


class CreateUser(StatefulFunction):
    def run(self, key: str, name: str):
        self.state.create(key, {'name': name, 'credit': 0})
        return key


class AddCredit(StatefulFunction):
    def run(self, key: str, credit: int):
        user_data = self.state.read(key)
        user_data['credit'] += credit
        self.state.update(key, user_data)


class SubtractCredit(StatefulFunction):
    def run(self, key: str, credit: int):
        user_data = self.state.read(key)
        user_data['credit'] -= credit
        self.state.update(key, user_data)
