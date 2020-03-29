
class Params(object):
    """A global object for our params"""
    def __init__(self, **kwargs):
        self.__dict__["_keys"] = kwargs

    def set_many(self, params):
        for k, v in params.items():
            self.__setattr__(k, v)

    def __getattr__(self, key):
        """Returns the attribute, may fail"""
        if key in self.__dict__: # Return the actual attribute if trying to access directly
            print("bypass %s" % key)
            return self.__dict__[key]
        return self._keys[key]

    def __setattr__(self, key, value):
        """Refuses to overwrite an existing entry"""
        if key in self._keys:
            print("%s already exists..." % key)
            raise AttributeError
        self._keys[key] = value

    def __str__(self):
        s = "PARAMS\n"
        for k, v in sorted(self._keys.items()):
            s += "   %s: %s\n" % (k, v)
        return s


PARAMS = Params()
