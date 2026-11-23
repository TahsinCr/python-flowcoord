"""Edge cases for deadlines, state, and cache primitives."""

from __future__ import annotations

import unittest

from _support import ManualClock

from flowcoord.timing import Deadline


class TimingEdgeTests(unittest.TestCase):
    def test_deadline_validation_and_unlimited_child(self):
        clock = ManualClock(4.0)
        deadline = Deadline.after(0, clock=clock)
        self.assertEqual(deadline.remaining, 0)
        self.assertTrue(deadline.expired)
        self.assertIs(deadline.child(None), deadline)
        absolute = Deadline.at(8, clock=clock)
        self.assertEqual(absolute.remaining, 4)
        clock.set(9)
        self.assertEqual(absolute.remaining, 0)
        with self.assertRaises(ValueError):
            Deadline.after(10**1000)
        with self.assertRaises(TypeError):
            Deadline.at(True)
        with self.assertRaises(ValueError):
            Deadline.at(float("inf"))
        with self.assertRaises(ValueError):
            Deadline.after(1, clock=lambda: float("nan"))
        bad_clock_deadline = Deadline.at(1, clock=lambda: float("inf"))
        with self.assertRaises(ValueError):
            _ = bad_clock_deadline.remaining
        with self.assertRaises(ValueError):
            Deadline(float("nan"))
