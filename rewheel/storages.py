from itertools import imap


class Storage(dict):
    """
    A Storage object is like a dictionary except `obj.foo` can be used
    in addition to `obj['foo']`, and setting obj.foo = None deletes item foo.
    """
    __slots__ = ()
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__
    __getitem__ = dict.get
    __getattr__ = dict.get
    __repr__ = lambda self: '<Storage %s>' % dict.__repr__(self)
    # http://stackoverflow.com/questions/5247250/why-does-pickle-getstate-accept-as-a-return-value-the-very-instance-it-requif: None
    __copy__ = lambda self: Storage(self)


class NestedDict(object):
    def __init__(self, main_dict, parent={}):
        self.main = {}
        self.parent = parent
        for key, value in main_dict.items():
            if type(value) is dict:
                self.main[key] = NestedDict(value, self)
            else:
                self.main[key] = value

    def __str__(self):
        return self.main.__str__()

    def __repr__(self):
        return self.main.__repr__()

    def __getitem__(self, item):
        if item in self.main:
            return self.main[item]
        return self.parent[item]
 
    def __setitem__(self, key, value):
        if key in self.main:
            self.main[key] = value
        elif key in self.parent:
            self.parent[key] = value
        else:
            self.main[key] = value

    def __contains__(self, item):
        return item in self.main or item in self.parent

    def __delitem__(self, key):
        if key in self.main:
            del self.main[key]
        elif self.parent:
            del self.parent[key]
        raise KeyError(key, '%s not found' % key)

    def __nonzero__(self):
        return bool(self.main) or bool(self.parent)

    def get(self, key, o=None):
        return self.main.get(key, o) or self.parent.get(key, o)

    @property
    def _root(self):
        root = self
        while self.parent:
            root = self.parent
        return root

    def _from_root(self):
        if self.parent:
            for x in self.parent._from_root():
                yield x
        yield self.main

    def keys(self):
        return sorted(reduce(set.union, imap(set, self._from_root())))

    def items(self):
        return self.to_dict().items()

    def iteritems(self):
        return self.to_dict().iteritems()

    def values(self):
        return list(self[k] for k in self.keys())

    def to_dict(self):
        r = {}
        for d in self._from_root():
            r.update(d)
        return r

    def __dict__(self):
        return self.main


    def __iter__(self):
        return iter(self.keys())



if __name__ == '__main__':
    import yaml
    with open('../flaskapp.yaml') as l:
        d = yaml.load(l)
    nd = NestedDict(d)
    print nd['logging'].to_dict()
    print nd['logging'].keys()
    print nd['logging'].values()


