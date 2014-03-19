"""
Hello Tiered Access

    >>> import pymongo
    >>> import datastore.core
    >>>
    >>> from datastore.impl.mongo import MongoDatastore
    >>> from datastore.impl.lrucache import LRUCache
    >>> from datastore.impl.filesystem import FileSystemDatastore
    >>>
    >>> conn = pymongo.Connection()
    >>> mongo = MongoDatastore(conn.test_db)
    >>>
    >>> cache = LRUCache(1000)
    >>> fs = FileSystemDatastore('/tmp/.test_db')
    >>>
    >>> ds = datastore.TieredDatastore([cache, mongo, fs])
    >>>
    >>> hello = datastore.Key('hello')
    >>> ds.put(hello, 'world')
    >>> ds.contains(hello)
    True
    >>> ds.get(hello)
    'world'
    >>> ds.delete(hello)
    >>> ds.get(hello)
    None

Hello Sharding

    >>> import datastore.core
    >>>
    >>> shards = [datastore.DictDatastore() for i in range(0, 10)]
    >>>
    >>> ds = datastore.ShardedDatastore(shards)
    >>>
    >>> hello = datastore.Key('hello')
    >>> ds.put(hello, 'world')
    >>> ds.contains(hello)
    True
    >>> ds.get(hello)
    'world'
    >>> ds.delete(hello)
    >>> ds.get(hello)
    None
"""
"""
serialize Hello World

    >>> import datastore.core
    >>> import json
    >>>
    >>> ds_child = datastore.DictDatastore()
    >>> ds = datastore.serialize.shim(ds_child, json)
    >>>
    >>> hello = datastore.Key('hello')
    >>> ds.put(hello, 'world')
    >>> ds.contains(hello)
    True
    >>> ds.get(hello)
    'world'
    >>> ds.delete(hello)
    >>> ds.get(hello)
    None
"""
