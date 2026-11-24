import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from mpmath import mp

from prime_experiment import Archive, check_manifest, pending_state, prediction_cell, score_cell
from publish_prime_score import refresh
from prime_models import MAX_INTEGER, endpoint, observed_psi, window


ASSETS = Path(__file__).resolve().parents[1] / "src/client_assets"


class PrimeResearchTests(unittest.TestCase):
    def test_observations_include_higher_powers_with_outside_bases(self):
        # 8 = 2^3 and 9 = 3^2. Counting only primes in the window gives zero.
        self.assertTrue(mp.almosteq(observed_psi(8, 9, []), mp.log(6)))
        self.assertEqual(observed_psi(14, 15, []), 0)

    def test_adjacent_endpoints_at_u64_maximum(self):
        n = MAX_INTEGER
        expected = mp.quad(lambda x: 1 / mp.log(x), [n-1, n])
        self.assertTrue(mp.almosteq(window(n, n)["li"], expected, rel_eps=mp.mpf("1e-28")))

    def test_revision_and_pagination_are_enforced(self):
        archive = Archive("http://localhost")
        def page(revision, n, cursor):
            return {"revision": revision, "next_cursor": cursor, "buckets": [
                {"lo": "2", "hi": "10", "covered": str(n), "count": "1"}]}
        archive.read = Mock(side_effect=[page("7", 5, "4"), page("7", 4, None)])
        self.assertEqual(archive.summary(2, 10, "7"), [[9, 2]])
        self.assertEqual(archive.read.call_args.kwargs["cursor"], 4)
        archive.read = Mock(return_value=page("8", 9, None))
        with self.assertRaises(ValueError):
            archive.summary(2, 10, "7")

    def test_partial_windows_stay_pending(self):
        archive = Mock()
        archive.summary.return_value = [[4, 2], [3, 1]]
        archive.primes.return_value = iter([11, 13])
        scored = score_cell(archive, "7", prediction_cell("fixture", 10, 4, 2))
        self.assertEqual([w["count"] for w in scored["windows"]], ["2"])
        self.assertEqual([w["covered"] for w in scored["pending"]], ["3"])
        archive.primes.assert_called_once_with(10, 13, "7")

    def test_freeze_refuses_archived_islands_and_active_leases(self):
        archive = Mock()
        cells = [{"lo": "100", "hi": "199"}]
        state = {"revision": "10", "verified_through": "99", "active": []}
        archive.read.return_value = state
        archive.summary.return_value = [[1, 0]]
        with self.assertRaisesRegex(ValueError, "archived"):
            pending_state(archive, cells)
        state["active"] = [{"lo": "190", "hi": "200"}]
        archive.summary.reset_mock()
        with self.assertRaisesRegex(ValueError, "lease"):
            pending_state(archive, cells)
        archive.summary.assert_not_called()

    def test_modified_prediction_rejected(self):
        manifest = json.loads((ASSETS / "prime-prediction.json").read_bytes())
        manifest["cells"][0]["windows"][0]["estimates"]["z64"] = "1"
        with self.assertRaisesRegex(ValueError, "checksum"):
            check_manifest(manifest)

    def test_publication_witness_precedes_score(self):
        forecast = (ASSETS / "prime-prediction.json").read_bytes()
        seed = json.loads((ASSETS / "prime-score.json").read_bytes())
        witness = {"manifest_sha256": seed["manifest_sha256"], "revision": seed["revision"]}
        with tempfile.TemporaryDirectory() as d:
            pages = Path(d)
            (pages / "prime-prediction.json").write_bytes(forecast)
            with patch("publish_prime_score.urlopen", return_value=io.BytesIO(b"stale")), \
                 patch("publish_prime_score.verify_pending") as capture:
                with self.assertRaisesRegex(ValueError, "Public manifest differs"):
                    refresh(pages, "a" * 40)
                capture.assert_not_called()
            with patch("publish_prime_score.urlopen", return_value=io.BytesIO(forecast)), \
                 patch("publish_prime_score.verify_pending", return_value=witness), \
                 patch("publish_prime_score.score", return_value=seed):
                refresh(pages, "a" * 40)
            saved = json.loads((pages / "prime-score.json").read_bytes())
            self.assertEqual(saved["publication"]["pages_commit"], "a" * 40)
            self.assertEqual(saved["publication"]["manifest_sha256"], saved["manifest_sha256"])

    def test_released_checkpoint_and_every_pinned_block(self):
        data = json.loads((ASSETS / "prime-experiment.json").read_bytes())
        self.assertEqual(data["revision"], "141726")
        self.assertEqual([(c["x"], c["count"]) for c in data["checkpoints"]],
                         [("1000000", "78498"), ("148607336447", "6021313753")])
        self.assertEqual(round(endpoint(148607336447)["rpsi64"]), 6021312704)
        block_cell, = [c for c in data["cells"] if c["width"] == 1048576
                       and c["hi"] == data["frontier"]["x"]]
        self.assertEqual([int(w["count"]) for w in block_cell["windows"]],
                         [40659, 40792, 40569, 40808, 40782, 40710, 40817, 40829])
        for cell in data["cells"]:
            for w in cell["windows"]:
                self.assertEqual(int(w["covered"]), int(w["hi"])-int(w["lo"])+1)


if __name__ == "__main__":
    unittest.main()
