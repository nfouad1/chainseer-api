from pathlib import Path
import unittest


class ApiDockerfileTests(unittest.TestCase):
    def test_public_network_adapters_are_packaged(self):
        dockerfile = (
            Path(__file__).resolve().parents[1] / "Dockerfile.api"
        ).read_text(encoding="utf-8")

        self.assertIn("chainseer_base_public.py", dockerfile)
        self.assertIn("chainseer_solana_public.py", dockerfile)
        self.assertIn("chainseer_api.py", dockerfile)


if __name__ == "__main__":
    unittest.main()
