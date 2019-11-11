
class Time:
    def __init__(self):
        self.T = 0

    def add(self, inc):
        self.T += inc

    def __str__(self):
        return str(self.T)
    def __int__(self):
        return int(self.T)
    def __float__(self):
        return float(self.T)
