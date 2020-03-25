import sys
import re
import traceback
from event import R

list_objs = (list, set, dict, tuple)
def infect(obj):
    if isinstance(obj, list_objs):
        for child in obj:
            print("recurse!")
            infect(child)

        obj = DebugList(obj)

    print(obj)
    if not hasattr(obj, "_infected"):
        try:
            print("infecting",  obj, end = "... ")
            obj._infected = True
            obj.__getattr__ = DebugLog.__getattr__
            obj.__setattr__ = DebugLog.__setattr__
            print("ok")
        except:
            print("fail")
            print(sys.exc_info()[0])
            print(sys.exc_info()[1])
            traceback.print_tb(sys.exc_info()[2])

    return obj

class DebugList:
    def __init__(self, obj):
        self.__dict__["_obj"] = obj
        self.__dict__["_infected"] = True

    def __getattr__(self, key):
        if key in self.__dict__:
            return self.__dict__[key]
        return self._obj.__getattribute__(key)
    def __setattr__(self, key, value):
        print("%s[%s] = %s" % (self, key, value))
        return self._obj.__setattr__(key, value)

    def __iter__(self):
        yield from self._obj

    def __getitem__(self, key):
        return self._obj[key]

    def __len__(self):
        return len(self._obj)

    def __setitem__(self, key, value):
        print("set %s[%s] = %s")
        self._obj[key] = value

def event(obj, event_type, key, value):
    try:
        obj_name = str(obj)
        ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
        obj_name = ansi_escape.sub('', obj_name)
    except:
        obj_name = "'" + repr(obj) + "'"

    print("%f, %s, %s, %s, %s, %s, \"%s\"" %
            (R.time, event_type,
                obj.__class__.__name__, id(obj), obj_name,
                key, value),
            file = sys.stderr)

def logfn(obj, fn):
    def log(*args, **kwargs):
        event(obj, "call", fn.__name__, (args, kwargs))
        return fn(*args, **kwargs)
    return log


class DebugLog:
    def __init__(self):
        self.__dict__["_infected"] = True

    def __getattr__(self, key):
        return self.__dict__[key]

    def __setattr__(self, key, value):
        if callable(value):
            value = logfn(self, value)

        event(self, "set", key, value)
        self.__dict__[key] = value

