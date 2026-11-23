"""Reviewed public signatures and exports."""

import inspect
import json
import unittest
from pathlib import Path

import flowcoord


class PublicContractTests(unittest.TestCase):
    def test_reviewed_public_apis_match_the_contract(self):
        contract = json.loads(
            Path(__file__).with_name("api_contract.json").read_text(encoding="utf-8")
        )
        for name in contract["exports"]:
            self.assertIn(name, flowcoord.__all__)
            self.assertIsNotNone(getattr(flowcoord, name))
        for name, definition in contract["signatures"].items():
            item = getattr(flowcoord, name)
            if "$call" in definition:
                self.assertEqual(str(inspect.signature(item)), definition["$call"], name)
            for method, signature in definition["methods"].items():
                self.assertEqual(
                    str(inspect.signature(getattr(item, method))),
                    signature,
                    f"{name}.{method}",
                )
