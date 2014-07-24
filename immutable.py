# encoding: utf-8
import weakref
import itertools

class imdict(object):
    def __init__(self, *args, **kwargs):
        print args, kwargs
        self.__dict = dict(*args, **kwargs)
        self.__forward_refs = []
        self.__backwards_ref = None
        self.__init__ = None

    def set(self, key, value):
        newdict = imdict({key: value})
        newdict._imdict__backwards_ref = weakref.ref(self)
        self.__forward_refs.append(newdict)
        return newdict

    def keys(self):
        if self.__backwards_ref:
            return self.__backwards_ref.keys() + self.__dict.keys()
        else:
            return self.__dict.keys()

    def iterkeys(self):
        if self.__backwards_ref:
            return itertools.chain(self.__backwards_ref.iterkeys(), self.__dict.iterkeys())
        else:
            return self.__dict.iterkeys()

    def values(self):
        if self.__backwards_ref:
            return self.__backwards_ref.values() + self.__dict.values()
        else:
            return self.__dict.values()

    def itervalues(self):
        if self.__backwards_ref:
            return itertools.chain(self.__backwards_ref.itervalues(), self.__dict.itervalues())
        else:
            return self.__dict.itervalues()

    def items(self):
        if self.__backwards_ref:
            return self.__backwards_ref.items() + self.__dict.items()
        else:
            return self.__dict.items()

    def iteritems(self):
        if self.__backwards_ref:
            return itertools.chain(self.__backwards_ref.iteritems(), self.__dict.iteritems())
        else:
            return self.__dict.iteritems()

    def __eq__(self, other):
        if isinstance(other, imdict):
            return self.items() == other.items()
        else:
            return False

    @classmethod
    def fromkeys(cls, *args, **kwargs):
        return cls(dict.fromkeys(*args, **kwargs))

    def __getitem__(self, key):
        if key in self.__dict:
            return self.__dict[key]
        if self.__backwards_ref:
            return self.__backwards_ref[key]
        else:
            raise KeyError(key)

    def __del__(self):
        for ref in self.__forward_refs:
            ref._imdict__dict.update(self.__dict)
            ref._imdict__backwards_ref = None

    def __repr__(self):
        return repr(self.__dict)