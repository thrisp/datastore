import uuid
from functools import total_ordering
from .utils import makehash

class Namespace(str):
    """A Key Namespace is a string identifier.

    A namespace can optionally include a field (delimited by ':')

    Example namespaces::

        Namespace('Bruces')
        Namespace('Song:PhilosopherSong')
    """
    namespace_delimiter = ':'

    def __repr__(self):
        return "Namespace('{!s}')".format(self)

    @property
    def field(self):
        """returns the `field` part of this namespace, if any."""
        if ':' in self:
            return self.split(self.namespace_delimiter)[0]
        return ''

    @property
    def value(self):
        """returns the `value` part of this namespace."""
        return self.split(self.namespace_delimiter)[-1]


@total_ordering
class Key(object):
    """A Key represents the unique identifier of an object.

    Key scheme is inspired by file systems and the Google App Engine key
    model.

    Keys are meant to be unique across a system. Keys are hierarchical,
    incorporating more and more specific namespaces. Thus keys can be deemed
    'children' or 'ancestors' of other keys::

        Key('/Comedy')
        Key('/Comedy/MontyPython')

    Also, every namespace can be parametrized to embed relevant object
    information. For example, the Key `name` (most specific namespace) could
    include the object type::

        Key('/Comedy/MontyPython/Actor:JohnCleese')
        Key('/Comedy/MontyPython/Sketch:CheeseShop')
        Key('/Comedy/MontyPython/Sketch:CheeseShop/Character:Mousebender')
    """
    __slots__ = ('_kstring', '_klist')

    def __init__(self, key):
        if isinstance(key, list):
            key = '/'.join(key)

        self._kstring = self.removeDuplicateSlashes(str(key))
        self._klist = None

    def __str__(self):
        """Returns the string representation of this Key."""
        return self._kstring

    def __repr__(self):
        """Returns the repr of this Key."""
        return "Key('{!s}')".format(self._kstring)

    def __iter__(self):
        return iter(self._kstring)

    def __len__(self):
        return len(self._kstring)

    def __eq__(self, other):
        if isinstance(other, Key):
            return self._kstring == other._kstring
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        if isinstance(other, Key):
            return (self._kstring < other._kstring)
        raise TypeError("other is not of type {!s}".format(Key))

    @property
    def klist(self):
        """Returns the `list` representation of this Key.

        Note that this method assumes the key is immutable.
        """
        if not self._klist:
            self._klist = list(map(Namespace, self._kstring.split('/')))
        return self._klist

    @property
    def reverse(self):
        """Returns the reverse of this Key.

            >>> Key('/Comedy/MontyPython/Actor:JohnCleese').reverse
            Key('/Actor:JohnCleese/MontyPython/Comedy')
        """
        return Key(self.klist[::-1])

    @property
    def namespaces(self):
        """Returns the list of namespaces of this Key."""
        return self.klist

    @property
    def name(self):
        """Returns the name of this Key, the value of the last namespace."""
        return Namespace(self.klist[-1]).value

    @property
    def type(self):
        """Returns the type of this Key, the field of the last namespace."""
        return Namespace(self.klist[-1]).field

    def instance(self, other):
        """Returns an instance Key, by appending a name to the namespace."""
        assert '/' not in str(other)
        return Key(str(self) + ':' + str(other))

    @property
    def path(self):
        """Returns the path of this Key, the parent and the type."""
        return Key(str(self.parent) + '/' + self.type)

    @property
    def parent(self):
        """Returns the parent Key (all namespaces except the last).

            >>> Key('/Comedy/MontyPython/Actor:JohnCleese').parent
            Key('/Comedy/MontyPython')
        """
        if '/' in self._kstring:
            return Key(self.klist[:-1])
        raise ValueError('{!r} is base key (it has no parent)'.format(self))

    def child(self, other):
        """Returns the child Key by appending namespace `other`.

            >>> Key('/Comedy/MontyPython').child('Actor:JohnCleese')
            Key('/Comedy/MontyPython/Actor:JohnCleese')
        """
        return Key('{!s}/{!s}'.format(self._kstring, str(other)))

    def isAncestorOf(self, other):
        """Returns whether this Key is an ancestor of `other`.

            >>> john = Key('/Comedy/MontyPython/Actor:JohnCleese')
            >>> Key('/Comedy').isAncestorOf(john)
            True
        """
        if isinstance(other, Key):
            return other._kstring.startswith(self._kstring + '/')
        raise TypeError('{} is not of type {}'.format(other, Key))

    def isDescendantOf(self, other):
        """Returns whether this Key is a descendant of `other`.

            >>> Key('/Comedy/MontyPython').isDescendantOf(Key('/Comedy'))
            True
        """
        if isinstance(other, Key):
            return other.isAncestorOf(self)
        raise TypeError('{} is not of type {}'.format(other, Key))

    def isTopLevel(self):
        """Returns whether this Key is top-level (one namespace)."""
        return len(self.list) == 1

    def __hash__(self):
        """Returns the hash of this Key.

        Note that for the purposes of this Key (that is, to use it and its hash
        values as unique identifiers across systems and platforms), the hash(.)
        builtin is not adequate (as it is not guaranteed to return the same hash
        value for two different interpreter runs, let alone different machines).

        For our purposes, then, we are using a perhaps more expensive hash function
        that guarantees equal hash values given the same input.
        """
        return makehash(self)

    @classmethod
    def random_key(cls):
        """Returns a random Key"""
        return Key(uuid.uuid4().hex)

    @classmethod
    def removeDuplicateSlashes(cls, path):
        """Returns the path string `path` without duplicate slashes."""
        return '/' + '/'.join(filter(lambda p: p != '', path.split('/')))
