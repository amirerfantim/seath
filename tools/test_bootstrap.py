import json
import unittest

from bootstrap import unique_object, validate_manifest


class ManifestTests(unittest.TestCase):
    def test_example(self):
        with open("configs/bootstrap.example.json") as source:
            modules, presets, mappings = validate_manifest(json.load(source))
        self.assertEqual((len(modules), len(presets), len(mappings)), (1, 1, 1))

    def test_reject_ambiguous_or_secret_input(self):
        preset = {"name": "local", "url": "nats://127.0.0.1:4222"}
        for manifest in (
            {"presets": [preset, preset]},
            {"presets": [{**preset, "password": "synthetic"}]},
            {"presets": [{**preset, "url": "nats://synthetic@localhost:4222"}]},
            {"presets": [{**preset, "url": "nats://localhost:4222?token=synthetic"}]},
            {"presets": [{**preset, "name": " local"}]},
            {"modules": [{"path": "example", "version": "v1\r\ninjection", "descriptor_file": "event.bin"}]},
            {"modules": "invalid"},
            {"mappings": {"example": None}},
            {"unknown": []},
        ):
            with self.subTest(manifest=manifest), self.assertRaises(RuntimeError):
                validate_manifest(manifest)

    def test_duplicate_json_keys(self):
        with self.assertRaises(RuntimeError):
            json.loads('{"mappings": {"example": "A", "example": "B"}}', object_pairs_hook=unique_object)


if __name__ == "__main__":
    unittest.main()
