# -*- coding:utf-8 -*-
import unittest
from mdmap import MDMap


class TestBigTable(unittest.TestCase):

    def test_massive_data(self):
        bt = MDMap(':memory:')

        for i in range(1000):
            bt.insert(str(i), "userId", "ABCDEFGH")
            bt.insert(str(i), "name", "Robert")
            bt.insert(str(i), "address", "Address Robert")

        for i in range(1000):
            bt.insert(str(i+1000), "userId", "XXYYXXYY")
            bt.insert(str(i+1000), "name", "Maike")
            bt.insert(str(i+1000), "address", "Address Maike")

        result = bt.select("SELECT name, addgress WHERE userId=ABCDEFGH OR userId=ADADADAD")
        self.assertEqual(1000, len(result))

    def test_logical_statement(self):
        bt = MDMap(':memory:')

        bt.insert(1, "user_id", "roberts user id")
        bt.insert(1, "name", "Robert")
        bt.insert(1, "address", "Address Robert")

        bt.insert(2, "user_id", "tims user id")
        bt.insert(2, "name", "Tim")
        bt.insert(2, "address", "Address Tim")

        bt.insert(3, "country", "Africa")
        bt.insert(3, "population", "2")

        bt.insert(4, "country", "Europe")
        bt.insert(4, "population", "1")

        result = bt.select("SELECT user_id, address WHERE name=Robert")

        self.assertEqual(1, len(result))

        result_dict = dict(result[0])
        self.assertTrue(u'row' in result_dict)
        self.assertTrue(u'user_id' in result_dict)
        self.assertTrue(u'address' in result_dict)

        self.assertEqual('roberts user id', result_dict[u'user_id'])
        self.assertEqual('1', result_dict[u'row'])
        self.assertEqual('Address Robert', result_dict[u'address'])

        result = bt.select("SELECT country, population WHERE population != 1")
        self.assertEqual(1, len(result))

        result_dict = dict(result[0])
        self.assertTrue(u'row' in result_dict)
        self.assertTrue(u'country' in result_dict)
        self.assertTrue(u'population' in result_dict)

        self.assertEqual('3', result_dict[u'row'])
        self.assertEqual('Africa', result_dict[u'country'])
        self.assertEqual('2', result_dict[u'population'])

        result = bt.select("SELECT country,population")
        self.assertEqual(2, len(result))
