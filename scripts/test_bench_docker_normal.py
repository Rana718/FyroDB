import unittest
from unittest.mock import patch

import bench_docker_normal as runner


class BenchmarkPolicyTests(unittest.TestCase):
    def test_lower_priority_native_client_cannot_silently_bias_a_run(self):
        with patch.object(runner.os, 'getpriority', side_effect=[0, 16]):
            with self.assertRaisesRegex(RuntimeError, 'priorities differ'):
                runner.verify_priorities(11, 22)

    def test_equal_process_priorities_are_recorded(self):
        with patch.object(runner.os, 'getpriority', side_effect=[0, 0]):
            self.assertEqual(runner.verify_priorities(11, 22), dict(server=0, client=0))

    def test_shared_cpus_need_an_explicit_workload_choice(self):
        with self.assertRaisesRegex(ValueError, 'overlap'):
            runner.validate_plan([0, 1], [1, 2], [1, 2])
        self.assertTrue(runner.validate_plan([0, 1], [1, 2], [1, 2], True))

    def test_worker_sweep_cannot_silently_oversubscribe_its_cpu_budget(self):
        with self.assertRaisesRegex(ValueError, 'worker counts'):
            runner.validate_plan([0, 1], [2, 3], [1, 2, 3])

    def test_disjoint_client_resources_are_valid(self):
        self.assertFalse(runner.validate_plan([0, 1, 2, 6, 7, 8], [3, 4, 5, 9, 10, 11], [1, 2, 3, 6]))


if __name__ == '__main__':
    unittest.main()
