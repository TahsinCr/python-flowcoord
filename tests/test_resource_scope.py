import unittest

from flowcoord.scopes import ResourceScope


class ResourceScopeEdgeTests(unittest.TestCase):
    def test_sync_scope_duplicate_borrowed_and_missing_detach(self):
        calls = []

        class Resource:
            def close(self):
                calls.append("closed")

        resource = Resource()
        scope = ResourceScope()
        self.assertIs(scope.add(resource), resource)
        scope.add(resource)
        self.assertIs(scope.detach(resource), resource)
        scope.add(resource)
        scope.add(resource)
        with self.assertRaises(KeyError):
            scope.detach(object())
        scope.close()
        scope.close()
        self.assertEqual(calls, ["closed"])
        with self.assertRaises(RuntimeError):
            scope.add(Resource())
        with self.assertRaises(TypeError):
            ResourceScope().add(Resource(), close="missing")
        with self.assertRaises(TypeError):
            ResourceScope().defer(object())
