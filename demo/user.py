from universalis_operator.opeartor import StatefulFunction


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
