#!/usr/bin/env python

from unittest import TestCase
from unittest.mock import patch

from dmyplant.dReliability import lipson_equality


class FakeResult:
    text = '<title>"Hello, World!" program - Wikipedia</title>'


class TestHelloWorld(TestCase):

    # @patch('requests.get')
    # def test_helloworld(self, mock_get):
    #     mock_get.return_value = FakeResult()

    #     do_hello()
    #     mock_get.assert_called_with(URL)

    def test_lipson_equality(self):
        return lipson_equality(24, 1000, 2000, 1.0)
