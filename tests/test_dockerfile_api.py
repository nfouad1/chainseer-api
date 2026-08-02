import ast
from pathlib import Path
import unittest


class ApiDockerfileTests(unittest.TestCase):
    def test_public_network_adapters_are_packaged(self):
        root = Path(__file__).resolve().parents[1]
        dockerfile = (root / "Dockerfile.api").read_text(encoding="utf-8")

        self.assertIn("chainseer_base_public.py", dockerfile)
        self.assertIn("chainseer_solana_public.py", dockerfile)
        self.assertIn("chainseer_api.py", dockerfile)

        pending = ["chainseer_api"]
        visited = set()
        while pending:
            module = pending.pop()
            if module in visited:
                continue
            visited.add(module)
            source_path = root / f"{module}.py"
            if not source_path.is_file():
                continue
            tree = ast.parse(source_path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                imported = None
                if isinstance(node, ast.ImportFrom):
                    imported = node.module
                elif isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name.startswith("chainseer_"):
                            pending.append(alias.name.split(".", 1)[0])
                if imported and imported.startswith("chainseer_"):
                    pending.append(imported.split(".", 1)[0])

        for module in visited:
            with self.subTest(module=module):
                self.assertIn(f"{module}.py", dockerfile)


if __name__ == "__main__":
    unittest.main()
