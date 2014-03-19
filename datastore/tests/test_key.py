from unittest import TestCase
import random

from datastore.key import Key
from datastore.key import Namespace

from . import TestKey


class KeyTest(TestKey):
    def test_basic(self):
        self.subtest_basic('')
        self.subtest_basic('abcde')
        self.subtest_basic('disahfidsalfhduisaufidsail')
        self.subtest_basic('/fdisahfodisa/fdsa/fdsafdsafdsafdsa/fdsafdsa/')
        self.subtest_basic(u'4215432143214321432143214321')
        self.subtest_basic('/fdisaha////fdsa////fdsafdsafdsafdsa/fdsafdsa/')
        self.subtest_basic('abcde:fdsfd')
        self.subtest_basic('disahfidsalfhduisaufidsail:fdsa')
        self.subtest_basic('/fdisahfodisa/fdsa/fdsafdsafdsafdsa/fdsafdsa/:')
        self.subtest_basic(u'4215432143214321432143214321:')
        self.subtest_basic('/fdisaha////fdsa////fdsafdsafdsafdsa/fdsafdsa/f:fdaf')

    def test_ancestry(self):
        k1 = Key('/A/B/C')
        k2 = Key('/A/B/C/D')

        self.assertEqual(k1._string, '/A/B/C')
        self.assertEqual(k2._string, '/A/B/C/D')
        self.assertTrue(k1.isAncestorOf(k2))
        self.assertTrue(k2.isDescendantOf(k1))
        self.assertTrue(Key('/A').isAncestorOf(k2))
        self.assertTrue(Key('/A').isAncestorOf(k1))
        self.assertFalse(Key('/A').isDescendantOf(k2))
        self.assertFalse(Key('/A').isDescendantOf(k1))
        self.assertTrue(k2.isDescendantOf(Key('/A')))
        self.assertTrue(k1.isDescendantOf(Key('/A')))
        self.assertFalse(k2.isAncestorOf(Key('/A')))
        self.assertFalse(k1.isAncestorOf(Key('/A')))
        self.assertFalse(k2.isAncestorOf(k2))
        self.assertFalse(k1.isAncestorOf(k1))
        self.assertEqual(k1.child('D'), k2)
        self.assertEqual(k1, k2.parent)
        self.assertEqual(k1.path, k2.parent.path)

    def test_type(self):
        k1 = Key('/A/B/C:c')
        k2 = Key('/A/B/C:c/D:d')

        self.assertRaises(TypeError, k1.isAncestorOf, str(k2))
        self.assertTrue(k1.isAncestorOf(k2))
        self.assertTrue(k2.isDescendantOf(k1))
        self.assertEqual(k1.type, 'C')
        self.assertEqual(k2.type, 'D')
        self.assertEqual(k1.type, k2.parent.type)

    def test_hashing(self):
        def randomKey():
            return Key('/herp/' + self.random_string() + '/derp')

        keys = {}

        for i in range(0, 200):
            key = randomKey()
            while key in keys.values():
                key = randomKey()

            hstr = str(hash(key))
            self.assertFalse(hstr in keys)
            keys[hstr] = key

        for key in keys.values():
            hstr = str(hash(key))
            self.assertTrue(hstr in keys)
            self.assertEqual(key, keys[hstr])

    def test_random(self):
        keys = set()
        for i in range(0, 1000):
            random = Key.randomKey()
            self.assertFalse(random in keys)
            keys.add(random)
        self.assertEqual(len(keys), 1000)


if __name__ == '__main__':
    unittest.main()
