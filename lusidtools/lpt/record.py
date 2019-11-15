# A simple key/value class with dot notation access
class Rec:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __str__(self):
        return str(self.__dict__)

    def __iter__(self):
        return self.__dict__.__iter__()

    def to_dict(self):
        return self.__dict__
