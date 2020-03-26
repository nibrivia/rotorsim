import sys
import re
import traceback
from event import R
from params import PARAMS

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

ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
def event(obj, event_type, key, value):
    try:
        obj_name = str(obj)
        obj_name = ansi_escape.sub('', obj_name)
    except:
        obj_name = "'" + repr(obj) + "'"

    print("%f, %s, %s, %s, %s, %s, \"%s\"" %
            (R.time, event_type,
                obj.__class__.__name__, id(obj), obj_name,
                key, value),
            file = sys.stderr)

def str_repr(x):
    try:
        s = str(x)
    except:
        s = repr(x)
    return ansi_escape.sub('', s)

def logfn(obj, fn):
    if "logfn" in fn.__qualname__:
        return fn

    def log(*args, **kwargs):
        pretty_args = ", ".join([str_repr(x) for x in args]) + " "
        pretty_kwargs = ", ".join(["%s = %s" % (k, str_repr(v)) for k, v in kwargs.items()])
        event(obj, "call", fn.__qualname__, pretty_args+ pretty_kwargs)
        return fn(*args, **kwargs)
    return log


class DebugLog:
    def __init__(self):
        self.__dict__["_infected"] = True

    def __getattr__(self, key):
        return self.__dict__[key]

    def __setattr__(self, key, value):
        if "_infected" not in self.__dict__:
            self.__dict__["_infected"] = True
            for k in dir(self):
                # skip dunders
                if "__" in k:
                    continue

                # make sure not to call properties...
                class_v = getattr(type(self), k, None)
                if isinstance(class_v, property):
                    continue

                v = getattr(self, k)
                if callable(v) and "log" not in v.__qualname__:
                    setattr(self, k, logfn(self, v))

        if callable(value):
            value = logfn(self, value)

        event(self, "set", key, value)
        self.__dict__[key] = value

if False:
    DebugLog = object
