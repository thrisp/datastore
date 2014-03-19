from unittest import TestCase

import hashlib
import nanotime

from ..key import Key
from ..query import Query


class TestKey(TestCase):
    def randomString(self):
        string = ''
        length = random.randint(0, 50)
        for i in range(0, length):
            string += chr(random.randint(ord('0'), ord('Z')))
        return string

    def subtest_basic(self, string):
        fixedString = Key.removeDuplicateSlashes(string)
        lastNamespace = fixedString.rsplit('/')[-1].split(':')
        ktype = lastNamespace[0] if len(lastNamespace) > 1 else ''
        name = lastNamespace[-1]
        path = fixedString.rsplit('/', 1)[0] + '/' + ktype
        instance = fixedString + ':' + 'c'

        self.assertEqual(Key(string)._string, fixedString)
        self.assertEqual(Key(string), Key(string))
        self.assertEqual(str(Key(string)), fixedString)
        self.assertEqual(repr(Key(string)), "Key('%s')" % fixedString)
        self.assertEqual(Key(string).name, name)
        self.assertEqual(Key(string).type, ktype)
        self.assertEqual(Key(string).instance('c'), Key(instance))
        self.assertEqual(Key(string).path, Key(path))
        self.assertEqual(Key(string), eval(repr(Key(string))))

        self.assertTrue(Kiey(string).child('a') > Key(string))
        self.assertTrue(Key(string).child('a') < Key(string).child('b'))
        self.assertTrue(Key(string) == Key(string))

        self.assertRaises(TypeError, cmp, Key(string), string)

        split = fixedString.split('/')
        if len(split) > 1:
            self.assertEqual(Key('/'.join(split[:-1])), Key(string).parent)
        else:
            self.assertRaises(ValueError, lambda: Key(string).parent)

        namespace = split[-1].split(':')
        if len(namespace) > 1:
            self.assertEqual(namespace[0], Key(string).type)
        else:
            self.assertEqual('', Key(string).type)


class TestDatastore(TestCase):
    pkey = Key('/dfadasfdsafdas/')

    def setUp(self):
        self.stores = []
        self.numelems = []

    def tearDown(self):
        self.stores = []
        self.numelems = []

    def check_length(self,len):
        try:
            for sn in self.stores:
                self.assertEqual(len(sn), len)
        except TypeError:
            pass

    def subtest_remove_nonexistent(self):
        self.assertTrue(len(self.stores) > 0)
        self.check_length(0)

        # ensure removing non-existent keys is ok.
        for value in range(0, self.numelems):
            key = self.pkey.child(value)
        for sn in self.stores:
            self.assertFalse(sn.contains(key))
            sn.delete(key)
            self.assertFalse(sn.contains(key))

        self.check_length(0)

    def subtest_insert_elems(self):
        # insert numelems elems
        for value in range(0, self.numelems):
            key = self.pkey.child(value)
            for store in self.stores:
                self.assertFalse(store.contains(key))
                store.put(key, value)
                self.assertTrue(store.contains(key))
                self.assertEqual(store.get(key), value)

        # reassure they're all there.
        self.check_length(self.numelems)

        for value in range(0, self.numelems):
            key = self.pkey.child(value)
        for store in self.stores:
            self.assertTrue(store.contains(key))
            self.assertEqual(store.get(key), value)

        self.check_length(self.numelems)

    def check_query(self, query, total, qslice):
        allitems = list(range(0, total))
        resultset = None

        for store in self.stores:
            try:
                expectedset = list(store.query(Query(self.pkey)))
                expected = expectedset[qslice]
                resultset = store.query(query)
                result = list(resultset)

                # make sure everything is there.
                self.assertTrue(len(expectedset) == len(allitems), '{!s} == {!s}'.format(str(expectedset), str(allitems)))
                self.assertTrue(all([val in expectedset for val in allitems]))
                self.assertTrue(len(result) == len(expected)), '{!s} == {!s}'.format(str(result), str(expected))
                self.assertTrue(all([val in result for val in expected]))

                #TODO: should order be preserved?
                #self.assertEqual(result, expected)

            except NotImplementedError:
                print('WARNING: {} does not implement query.'.format(store))

        return resultset

    def subtest_queries(self):
        for value in range(0, self.numelems):
            key = self.pkey.child(value)
            for store in self.stores:
                store.put(key, value)

        k = self.pkey
        n = int(self.numelems)

        self.check_query(Query(k), n, slice(0, n))
        self.check_query(Query(k, limit=n), n, slice(0, n))
        self.check_query(Query(k, limit=(n/2)), n, slice(0, int(n/2)))
        self.check_query(Query(k, offset=n/2), n, slice(int(n/2), n))
        self.check_query(Query(k, offset=n/3, limit=n/3), n, slice(int(n/3), int(2*(n/3))))
        del k
        del n

    def subtest_update(self):
        # change numelems elems
        for value in range(0, self.numelems):
            key = self.pkey.child(value)
            for store in self.stores:
                self.assertTrue(store.contains(key))
                store.put(key, value + 1)
                self.assertTrue(store.contains(key))
                self.assertNotEqual(value, store.get(key))
                self.assertEqual(value + 1, store.get(key))

        self.check_length(self.numelems)

    def subtest_remove(self):
        # remove numelems elems
        for value in range(0, self.numelems):
            key = self.pkey.child(value)
            for store in self.stores:
                self.assertTrue(store.contains(key))
                store.delete(key)
                self.assertFalse(store.contains(key))

        self.check_length(0)

    def subtest_simple(self, stores, numelems=100):
        self.stores = stores
        self.numelems = numelems

        self.subtest_remove_nonexistent()
        self.subtest_insert_elems()
        self.subtest_queries()
        self.subtest_update()
        self.subtest_remove()


class TestNullDatastore(TestCase):
    def test_null(self):
        from ..basic import NullDatastore

        s = NullDatastore()

        for c in range(1, 20):
            c = str(c)
            k = Key(c)
            self.assertFalse(s.contains(k))
            self.assertEqual(s.get(k), None)
            s.put(k, c)
            self.assertFalse(s.contains(k))
            self.assertEqual(s.get(k), None)

        for item in s.query(Query(Key('/'))):
            raise Exception('Should not have found anything.')


class TestQuery(TestCase):
    def version_objects(self):
        sr1 = {}
        sr1['key'] = '/ABCD'
        sr1['hash'] = hashlib.sha1('herp'.encode('utf-8')).hexdigest()
        sr1['parent'] = '0000000000000000000000000000000000000000'
        sr1['created'] = nanotime.now().nanoseconds()
        sr1['committed'] = nanotime.now().nanoseconds()
        sr1['attributes'] = {'str' : {'value' : 'herp'} }
        sr1['type'] = 'Hurr'

        sr2 = {}
        sr2['key'] = '/ABCD'
        sr2['hash'] = hashlib.sha1('derp'.encode('utf-8')).hexdigest()
        sr2['parent'] = hashlib.sha1('herp'.encode('utf-8')).hexdigest()
        sr2['created'] = nanotime.now().nanoseconds()
        sr2['committed'] = nanotime.now().nanoseconds()
        sr2['attributes'] = {'str' : {'value' : 'derp'} }
        sr2['type'] = 'Hurr'

        sr3 = {}
        sr3['key'] = '/ABCD'
        sr3['hash'] = hashlib.sha1('lerp'.encode('utf-8')).hexdigest()
        sr3['parent'] = hashlib.sha1('derp'.encode('utf-8')).hexdigest()
        sr3['created'] = nanotime.now().nanoseconds()
        sr3['committed'] = nanotime.now().nanoseconds()
        sr3['attributes'] = {'str' : {'value' : 'lerp'} }
        sr3['type'] = 'Hurr'

        return sr1, sr2, sr3
