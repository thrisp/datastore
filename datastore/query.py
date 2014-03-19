from copy import copy
from functools import total_ordering
from operator import itemgetter

from .key import Key


def _object_getattr(obj, field):
    """Attribute getter for the objects to operate on.

    This function can be overridden in classes or instances of Query, Filter, or
    Order. Thus, a custom function to extract values to attributes can be
    specified, and the system can remain agnostic to the client's data model,
    without losing query power.

    For example, the default implementation works with attributes and items:

        def _object_getattr(obj, field):
            # check whether this key is an attribute
            if hasattr(obj, field):
                value = getattr(obj, field)

            # if not, perhaps it is an item (raw dicts, etc)
            elif field in obj:
                value = obj[field]

            # return whatever we've got.
            return value

    Or consider a more application-specific use-case:

        def _object_getattr(version, field):
            if field in ['key', 'committed', 'created', 'hash']:
                return getattr(version, field)
            else:
                return version.attributes[field]['value']
    """
    value = None

    if hasattr(obj, field):
        value = getattr(obj, field)
    elif field in obj:
        value = obj[field]

    return value


def limit_gen(limit, iterable):
    """A generator that applies a count limit."""
    limit = int(limit)
    assert limit >= 0, 'negative limit'

    for item in iterable:
        if limit <= 0:
            break
        yield item
        limit -= 1


def offset_gen(offset, iterable, skip_signal=None):
    """A generator that applies an offset, skipping offset elements from
    iterable. If skip_signal is a callable, it will be called with every
    skipped element.
    """
    offset = int(offset)
    assert offset >= 0, 'negative offset'

    for item in iterable:
        if offset > 0:
            offset -= 1
            if callable(skip_signal):
                skip_signal(item)
        else:
            yield item


def chain_gen(iterables):
    """A generator that chains iterables."""
    for iterable in iterables:
        for item in iterable:
            yield item


def is_iterable(obj):
    return hasattr(obj, '__iter__') or hasattr(obj, '__getitem__')


class Filter(object):
    """Represents a Filter for a specific field and its value.
    Filters are used on queries to narrow down the set of matching objects.

    :param field:  the attribute name (string) on which to apply the filter
    :param op:     one of ['<', '<=', '=', '!=', '>=', '>'] conditional operators
    :param value:  the attribute value to compare against

    e.g.
        Filter('name', '=', 'John Cleese')
        Filter('age', '>=', 18)
    """
    conditional_operators = ['<', '<=', '=', '!=', '>=', '>']

    _conditional_cmp = {
        "<"  : lambda a, b: a < b,
        "<=" : lambda a, b: a <= b,
        "="  : lambda a, b: a == b,
        "!=" : lambda a, b: a != b,
        ">=" : lambda a, b: a >= b,
        ">"  : lambda a, b: a > b
    }

    # Object attribute getter. Can be overridden to match client data model.
    # See :py:meth:`datastore.query._object_getattr`.
    object_getattr = staticmethod(_object_getattr)

    def __init__(self, field, op, value):
        if op not in self.conditional_operators:
            raise ValueError('"{}" is not a valid filter Conditional Operator'.format(op))

        self.field = field
        self.op = op
        self.value = value

    def __call__(self, obj):
        """Returns whether this object passes this filter.
        This method aggressively tries to find the appropriate value.
        """
        value = self.object_getattr(obj, self.field)

        # TODO: which way should the direction go here? it may make more sense to
        #       convert the passed-in value instead. Or try both? Or not at all?
        if not isinstance(value, self.value.__class__) and not self.value is None and not value is None:
            value = self.value.__class__(value)

        return self.valuePasses(value)

    def valuePasses(self, value):
        """Returns whether this value passes this filter"""
        return self._conditional_cmp[self.op](value, self.value)

    def __str__(self):
        return '{} {} {}'.format(self.field, self.op, self.value)

    def __repr__(self):
        return "Filter({!r}, {!r}, {!r})".format(self.field, self.op, self.value)

    def __eq__(self, other):
        return self.field == other.field \
            and self.op == other.op \
            and self.value == other.value

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(repr(self))

    def generator(self, iterable):
        """Generator function that iteratively filters given items."""
        for item in iterable:
            if self(item):
                yield item

    @classmethod
    def filter(cls, filters, iterable):
        """Returns the elements in iterable that pass given filters"""
        if isinstance(filters, Filter):
            filters = [filters]

        for filter in filters:
            iterable = filter.generator(iterable)

        return iterable


class Order(object):
    """Represents an Order upon a specific field, and a direction.
    Orders are used on queries to define how they operate on objects

    :param order: an order in string form. This follows the format: [+-]name
                  where + is ascending, - is descending, and name is the name
                  of the field to order by.

    Examples:

        Order('+name')      #  ascending order by name
        Order('-age')       #  descending order by age
        Order('+score')     #  ascending order by score
    """
    order_operators = ['-', '+']
    object_getattr = staticmethod(_object_getattr)

    def __init__(self, order):
        if order[0] not in self.order_operators:
            raise ValueError("{} order operator must be one of '+' or '-'".format(order[0]))

        self.op = order[0]

        try:
            self.field = order[1:]
        except IndexError:
            raise ValueError("specified field must be at least two characters")

    def __str__(self):
        return "{}{}".format(self.op, self.field)

    def __repr__(self):
        return "Order('{}{}')".format(self.op, self.field)

    def __eq__(self, other):
        return self.field == other.field and self.op == other.op

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(repr(self))

    @property
    def descending(self):
        return self.op == '-'

    def keyfn(self, obj):
        """A key function to be used in pythonic sort operations."""
        return self.object_getattr(obj, self.field)

    @classmethod
    def sort_step(cls, items, order):
        #what is items aren't dicts as in tests
        return sorted(items, key=itemgetter(order.field), reverse=order.descending)

    @classmethod
    def sort_orders(cls, items, orders):
        """Returns the elements in items sorted according to orders"""
        for o in orders:
            items = cls.sort_step(items, o)
        return items


class Query(object):
    """A Query describes a set of objects.

    Queries are used to retrieve objects and instances matching a set of criteria
    from Datastores. Query objects themselves are simply descriptions,
    the actual Query implementations are left up to the Datastores.
    """

    #Object attribute getter. Can be overridden to match client data model.
    object_getattr = staticmethod(_object_getattr)

    def __init__(self, key=Key('/'), object_getattr=None, **kwargs):
        self.key = self.check_key(key)
        if object_getattr:
            self.object_getattr = object_getattr
        self.limit = self.check_limit(kwargs.get('limit', None))
        self.offset = int(kwargs.get('offset', 0))
        self.offset_key = kwargs.get('offset_key', None)
        self.filters = []
        self.orders = []

    def check_key(self, key):
        if isinstance(key, Key):
            return key
        else:
            raise TypeError('key must be of type {}'.format(Key))

    def check_limit(self, limit):
        if limit==0 or limit:
            return int(limit)
        else:
            return None

    def __str__(self):
        """Returns a string describing this query."""
        return repr(self)

    def __repr__(self):
        """Returns the representation of this query. Enables eval(repr(.))."""
        return 'Query.from_dict({!s})'.format(self.to_dict())

    def __call__(self, iterable):
        """Naively apply this query on an iterable of objects.
        Applying a query applies filters, sorts by appropriate orders, and returns
        a limited set.

        WARNING: When orders are applied, this function operates on the entire set
                of entities directly, not just iterators/generators. That means
                the entire result set will be in memory. Datastores with large
                objects and large query results should translate the Query and
                perform their own optimizations.
        """

        cursor = Cursor(self, iterable)
        cursor.apply_filter()
        cursor.apply_order()
        cursor.apply_offset()
        cursor.apply_limit()
        return cursor

    def __hash__(self):
        return hash(repr(self))

    def add_order(self, order):
        """Adds an Order to this query.

        Args:
        see :py:class:`Order <datastore.query.Order>` constructor

        Returns self for JS-like method chaining::

        query.order('+age').order('-home')

        """
        order = order if isinstance(order, Order) else Order(order)

        # ensure order gets attr values the same way the rest of the query does.
        order.object_getattr = self.object_getattr
        self.orders.append(order)
        return self # for chaining

    def add_filter(self, *args):
        """Adds a Filter to this query. Returns self, and is chainable.

        :py:class:`Filter <datastore.query.Filter>` constructor

        e.g.

            query.filter('age', '>', 18).filter('sex', '=', 'Female')
        """
        if len(args) == 1 and isinstance(args[0], Filter):
            f = args[0]
        else:
            f = Filter(*args)

        f.object_getattr = self.object_getattr

        self.filters.append(f)

        return self

    def copy(self):
        """Returns a copy of this query."""
        return copy(self)

    def to_dict(self):
        """Returns a dictionary representing this query."""
        d = dict()
        d['key'] = str(self.key)

        if self.limit is not None:
            d['limit'] = self.limit
        if self.offset > 0:
            d['offset'] = self.offset
        if self.offset_key:
            d['offset_key'] = str(self.offset_key)
        if len(self.filters) > 0:
            d['filter'] = [[f.field, f.op, f.value] for f in self.filters]
        if len(self.orders) > 0:
            d['order'] = [str(o) for o in self.orders]

        return d

    @classmethod
    def from_dict(cls, dictionary):
        """Constructs a query from a dictionary."""
        query = cls(Key(dictionary['key']))

        for key, value in dictionary.items():
            if key == 'order':
                for order in value:
                    query.add_order(order)
            elif key == 'filter':
                for f in value:
                    if not isinstance(f, Filter):
                        f = Filter(*f)
                    query.add_filter(f)
            elif key in ['limit', 'offset', 'offset_key']:
                setattr(query, key, value)
        return query


class Cursor(object):
    """Represents a query result generator."""

    __slots__ = ('query', '_iterable', '_iterator', 'skipped', 'returned', )

    def __init__(self, query, iterable):
        if not isinstance(query, Query):
            raise ValueError('Cursor received invalid query: {!s}'.format(query))

        if not is_iterable(iterable):
            raise ValueError('Cursor received invalid iterable: {!s}'.format(iterable))

        self.query = query
        self._iterable = iterable
        self._iterator = None
        self.returned = 0
        self.skipped = 0

    def __iter__(self):
        """The cursor itself is the iterator. Note that it cannot be used twice,
        and once iteration starts, the cursor cannot be modified.
        """
        if self._iterator:
            raise RuntimeError('Attempt to iterate over Cursor twice.')

        self._iterator = iter(self._iterable)
        return self

    def __next__(self):
        """Iterator next. Build up count of returned elements during iteration."""

        # if iteration has not begun, begin it.
        if not self._iterator:
            self.__iter__()

        nxt = next(self._iterator)
        if nxt is not StopIteration:
            self._returned_inc(nxt)
        return nxt

    def next(self):
        return self.__next__()

    def _skipped_inc(self, item):
        """A function to increment the skipped count."""
        self.skipped += 1

    def _returned_inc(self, item):
        """A function to increment the returned count."""
        self.returned += 1

    def _ensure_modification_is_safe(self):
        """Assertions to ensure modification of this Cursor is safe."""
        assert self.query, 'Cursor must have a Query.'
        assert is_iterable(self._iterable), 'Cursor must have a resultset iterable.'
        assert not self._iterator, 'Cursor must not be modified after iteration.'

    def apply_filter(self):
        """Naively apply query filters."""
        self._ensure_modification_is_safe()

        if len(self.query.filters) > 0:
            self._iterable = Filter.filter(self.query.filters, self._iterable)

    def apply_order(self):
        """Naively apply query orders."""
        self._ensure_modification_is_safe()

        if len(self.query.orders) > 0:
            self._iterable = Order.sort_orders(self._iterable, self.query.orders)

    def apply_offset(self):
        """Naively apply query offset."""
        self._ensure_modification_is_safe()

        if self.query.offset != 0:
            self._iterable = offset_gen(self.query.offset, self._iterable, self._skipped_inc)
            # _skipped_inc helps keep count of skipped elements

    def apply_limit(self):
        """Naively apply query limit."""
        self._ensure_modification_is_safe()
        if self.query.limit is not None:
            self._iterable = limit_gen(self.query.limit, self._iterable)
