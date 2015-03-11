# -*- coding:utf-8 -*-
import sqlite3
import time
import itertools
import pyparsing as pp

from operator import itemgetter


class MDMap():

    def __init__(self, db_path='example.db'):
        self.conn = sqlite3.connect(db_path)
        self.syntax_parser = self.statement_parser()

        c = self.conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS data_store(row text, column text, time text, value text)''')

    def insert(self, row_key, column, value):
        c = self.conn.cursor()
        c.execute("INSERT INTO data_store VALUES (?, ?, ?, ?)", [row_key, column, str(time.time()), value])
        c.close()

    @staticmethod
    def parse_where_elements(element, idx):
        if idx % 2 == 0 and len(element) == 3:
            return "(column='{}' AND value{}'{}')".format(element[0], element[1], element[2])
        else:
            return element

    def find_involved_rows(self, where_part):
        c = self.conn.cursor()
        if len(where_part) > 0:
            result = [MDMap.parse_where_elements(where_part[i], i) for i in range(len(where_part))]
            appendix = " ".join(result)
            c.execute("SELECT distinct(row) FROM data_store WHERE {}".format(appendix))
        else:
            c.execute("SELECT distinct(row) FROM data_store")
        conditional_rows = c.fetchall()
        c.close()
        return conditional_rows

    def select(self, command):
        # First check if the requested statement is syntactical correct
        result = self.syntax_parser.parseString(command)

        select_part = result.get("SELECT", [])
        where_part = result.get("WHERE", [])

        # Find all involved rows
        conditional_rows = self.find_involved_rows(where_part)

        # Check if we even have rows that fulfill this condition
        if len(conditional_rows) == 0:
            return None

        c = self.conn.cursor()

        fields = ["column='{}'".format(e.strip()) for e in select_part]
        condition = " OR ".join(fields)

        row_elements = [int(e[0]) for e in conditional_rows]
        total_result = []
        while len(row_elements) > 0:
            row_elements_to_use = row_elements[0:200]
            row_elements = row_elements[200:]

            row_elements_to_use = ["row='{}'".format(idx) for idx in row_elements_to_use]
            idxs = " OR ".join(row_elements_to_use)

            statement = "SELECT row, column, value FROM data_store WHERE ({}) AND ({})".format(condition, idxs)

            c.execute(statement)
            total_result += c.fetchall()

        total_result = sorted(total_result, key=lambda element: int(element[0]))

        return [[("row", result_element[0])] + [(e[1], e[2]) for e in list(result_element[1])]
                for result_element in itertools.groupby(total_result, key=itemgetter(0))]

    @classmethod
    def statement_parser(cls):
        where = pp.Literal('WHERE').suppress()
        select = pp.Literal('SELECT')
        comma = pp.Literal(',').suppress()
        operator_and = pp.Literal('AND')
        operator_or = pp.Literal('OR')
        num = pp.Word(pp.alphanums+"_")
        equals = pp.Literal('=') | pp.Literal('>') | pp.Literal('<') | pp.Literal('<=') | pp.Literal('>=') | pp.Literal('!=')
        group = pp.Group(num + equals + num)
        expr = pp.Forward()
        expr << select + pp.Group(num + pp.ZeroOrMore(comma + num)).setResultsName('SELECT') + pp.Optional(where + group + pp.ZeroOrMore((operator_and | operator_or) + group)).setResultsName('WHERE') + pp.WordEnd()
        return expr








