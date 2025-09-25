import unittest

from buildtool.views.sprint_helpers import filter_users_by_role


class FilterUsersByRoleTest(unittest.TestCase):
    def test_returns_all_users_when_role_not_required(self):
        users = ["alice", "bob"]
        roles = {"alice": ["developer"], "bob": ["qa"]}
        self.assertEqual(filter_users_by_role(users, roles, None), users)

    def test_includes_leaders_when_filtering_by_role(self):
        users = ["alice", "bob", "carol", "dave"]
        roles = {
            "alice": ["developer"],
            "bob": ["qa"],
            "carol": ["leader"],
            "dave": ["Developer", "Leader"],
        }

        devs = filter_users_by_role(users, roles, "developer")
        self.assertEqual(devs, ["alice", "carol", "dave"])

        qas = filter_users_by_role(users, roles, "qa")
        self.assertEqual(qas, ["bob", "carol", "dave"])
