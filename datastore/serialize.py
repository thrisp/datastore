import json
from .basic import Datastore, ShimDatastore


default_serializer = json


class Serializer(object):
    """Serializing protocol. Serialized data must be a string."""
    @classmethod
    def loads(cls, value):
        """returns deserialized value."""
        raise NotImplementedError

    @classmethod
    def dumps(cls, value):
        """returns serialized value."""
        raise NotImplementedError

    @staticmethod
    def implements_serializer_interface(cls):
        return hasattr(cls, 'loads') and callable(cls.loads) \
            and hasattr(cls, 'dumps') and callable(cls.dumps)


class NonSerializer(Serializer):
    """Implements serializing protocol but does not serialize at all.
    For only storing strings (or already-serialized values).
    """
    @classmethod
    def loads(cls, value):
        """returns value."""
        return value

    @classmethod
    def dumps(cls, value):
        """returns value."""
        return value


class prettyjson(Serializer):
    """json wrapper serializer that pretty-prints.
    Useful for human readable values and versioning.
    """
    @classmethod
    def loads(cls, value):
        """returns json deserialized value."""
        return json.loads(value)

    @classmethod
    def dumps(cls, value):
        """returns json serialized value, pretty-printed."""
        return json.dumps(value, sort_keys=True, indent=1)


class Stack(Serializer, list):
    """represents a stack of serializers, applying each serializer in sequence."""
    def loads(self, value):
        """Returns deserialized value."""
        for serializer in reversed(self):
            value = serializer.loads(value)
        return value

    def dumps(self, value):
        """returns serialized value."""
        for serializer in self:
            value = serializer.dumps(value)
        return value


class MapSerializer(Serializer):
    """map serializer that ensures the serialized value is a mapping type."""
    sentinel = '@wrapped'

    @classmethod
    def loads(cls, value):
        """Returns mapping type deserialized value."""
        if len(value) == 1 and cls.sentinel in value:
            value = value[cls.sentinel]
        return value

    @classmethod
    def dumps(cls, value):
        """returns mapping typed serialized value."""
        if not hasattr(value, '__getitem__') or not hasattr(value, 'iteritems'):
            value = {cls.sentinel: value}
        return value


def deserialized_gen(serializer, iterable):
    """Generator that yields deserialized objects from iterable."""
    for item in iterable:
        yield serializer.loads(item)


def serialized_gen(serializer, iterable):
    """Generator that yields serialized objects from iterable."""
    for item in iterable:
        yield serializer.dumps(item)


def monkey_patch_bson(bson=None):
    """Patch bson in pymongo to use loads and dumps interface."""
    if not bson:
        import bson

    if not hasattr(bson, 'loads'):
        bson.loads = lambda bsondoc: bson.BSON(bsondoc).decode()

    if not hasattr(bson, 'dumps'):
        bson.dumps = lambda document: bson.BSON.encode(document)


class SerializerShimDatastore(ShimDatastore):
    """Represents a Datastore that serializes and deserializes values.

    As data is put, the serializer shim serializes it and puts it into
    the underlying child_datastore. On the way out (through get or query)
    the data is retrieved from the child_datastore and deserialized.

    :param datastore: a child datastore for the ShimDatastore superclass.
    :param serializer: a serializer object (responds to loads and dumps).
    """
    # override this with their own custom serializer on a class-wide or per-
    # instance basis. If you plan to store mostly strings, use NonSerializer.
    serializer = default_serializer

    def __init__(self, datastore, serializer=None):
        """Initializes internals and tests the serializer."""
        super(SerializerShimDatastore, self).__init__(datastore)

        if serializer:
            self.serializer = serializer

        # ensure serializer works
        test = { 'value': repr(self) }
        errstr = 'Serializer error: serialized value does not match original'
        assert self.serializer.loads(self.serializer.dumps(test)) == test, errstr

    def serializedValue(self, value):
        """Returns serialized value or None."""
        return self.serializer.dumps(value) if value is not None else None

    def deserializedValue(self, value):
        """Returns deserialized value or None."""
        return self.serializer.loads(value) if value is not None else None

    def get(self, key):
        """Return the object named by key or None if it does not exist.
        Retrieves the value from the child_datastore, and de-serializes
        it on the way out.

        :param key: Key object naming the object to retrieve
        """
        value = self.child_datastore.get(key)
        return self.deserializedValue(value)

    def put(self, key, value):
        """Stores the object value named by key. Serializes values on the way in,
        and stores the serialized data into the child_datastore.

        :param key: Key naming value
        :param value: the object to store.
        """
        value = self.serializedValue(value)
        self.child_datastore.put(key, value)

    def query(self, query):
        """Returns an iterable of objects matching criteria expressed in query.
        De-serializes values on the way out, using deserialized_gen to
        avoid incurring the cost of de-serializing all data at once, or ever,
        if iteration over results does not finish (subject to order generator
        constraint).

        :param query: Query object describing the objects to return.
        """
        # run the query on the child datastore
        cursor = self.child_datastore.query(query)

        # chain the deserializing generator to the cursor's result set iterable
        cursor._iterable = deserialized_gen(self.serializer, cursor._iterable)

        return cursor


def shim(datastore, serializer=None):
    """Return a SerializerShimDatastore wrapping datastore.

    Can be used as a syntacticly-nicer eay to wrap a datastore with a
    serializer:

    e.g.

        my_store = datastore.serialize.shim(my_store, json)
    """
    return SerializerShimDatastore(datastore, serializer=serializer)


