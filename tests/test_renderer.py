from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch
import os

from PIL import Image

from printfarm.models import PresetConfig, TaskItem, WorkerConfig
from printfarm.renderer import Renderer, _icc_fingerprint


class RecordingRenderer(Renderer):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.color_transform_input_sizes: list[tuple[int, int]] = []

    def _apply_color_transform(self, image: Image.Image, worker: WorkerConfig, preset: PresetConfig) -> Image.Image:
        self.color_transform_input_sizes.append(image.size)
        return image.convert("RGB")


def worker(root: Path) -> WorkerConfig:
    preset = PresetConfig(name="default")
    return WorkerConfig(
        name="WorkerA",
        directory=root,
        printer_name="PrinterA",
        active_preset=preset.name,
        presets={preset.name: preset},
    )


class RendererExifOrientationTests(TestCase):
    def test_exif_orientation_is_applied_before_caching(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            image_path = root / "rotated.jpg"
            exif = Image.Exif()
            exif[0x0112] = 6  # stored rotated 90°: viewers rotate it upright
            Image.new("RGB", (40, 20), (200, 10, 10)).save(image_path, exif=exif.tobytes(), dpi=(100, 100))
            renderer = RecordingRenderer(root / "cache", rip_limit_enabled=False)

            artifact = renderer.ensure_render_cache(TaskItem(file_path=image_path), worker(root))

            # Upright pixels: width/height swapped relative to the stored bitmap.
            with Image.open(artifact.page_paths[0]) as output:
                self.assertEqual(output.size, (20, 40))
            page = artifact.metadata["pages"][0]
            self.assertAlmostEqual(page["width_mm"], 20 / 100 * 25.4, places=2)
            self.assertAlmostEqual(page["height_mm"], 40 / 100 * 25.4, places=2)

    def test_image_without_orientation_tag_is_unchanged(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            image_path = root / "plain.jpg"
            Image.new("RGB", (40, 20), (10, 200, 10)).save(image_path, dpi=(100, 100))
            renderer = RecordingRenderer(root / "cache", rip_limit_enabled=False)

            artifact = renderer.ensure_render_cache(TaskItem(file_path=image_path), worker(root))

            with Image.open(artifact.page_paths[0]) as output:
                self.assertEqual(output.size, (40, 20))


class RendererImageRipPreshrinkTests(TestCase):
    def test_large_image_is_preshrunk_before_color_transform_then_final_limited(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            image_path = root / "large.png"
            Image.new("RGB", (1200, 600), (128, 64, 32)).save(image_path, dpi=(300, 300))
            renderer = RecordingRenderer(root / "cache", rip_limit_enabled=True, rip_limit_ppi=100)

            artifact = renderer.ensure_render_cache(TaskItem(file_path=image_path), worker(root))

            self.assertEqual(renderer.color_transform_input_sizes, [(800, 400)])
            with Image.open(artifact.page_paths[0]) as output:
                self.assertEqual(output.size, (400, 200))

    def test_image_within_preshrink_window_keeps_original_size_until_color_transform(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            image_path = root / "moderate.png"
            Image.new("RGB", (700, 350), (32, 64, 128)).save(image_path, dpi=(150, 150))
            renderer = RecordingRenderer(root / "cache", rip_limit_enabled=True, rip_limit_ppi=100)

            artifact = renderer.ensure_render_cache(TaskItem(file_path=image_path), worker(root))

            self.assertEqual(renderer.color_transform_input_sizes, [(700, 350)])
            with Image.open(artifact.page_paths[0]) as output:
                self.assertEqual(output.size, (467, 233))

    def test_preshrink_preserves_image_info_for_later_color_handling(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            renderer = Renderer(root / "cache", rip_limit_enabled=True, rip_limit_ppi=100)
            image = Image.new("RGB", (1200, 600), (255, 255, 255))
            image.info["icc_profile"] = b"fake-profile"

            shrunk = renderer._pre_shrink_for_rip_limit(
                image,
                width_mm=4 * 25.4,
                height_mm=2 * 25.4,
                source_path=root / "source.png",
                worker=worker(root),
            )

            self.assertEqual(shrunk.size, (800, 400))
            self.assertEqual(shrunk.info.get("icc_profile"), b"fake-profile")

    def test_cmyk_without_icc_error_defaults_to_english_and_can_localize(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            image = Image.new("CMYK", (4, 4))
            current_worker = worker(root)
            preset = current_worker.get_active_preset()

            with self.assertRaisesRegex(RuntimeError, "WorkerA: CMYK input has no available ICC"):
                Renderer(root / "cache-en")._apply_color_transform(image, current_worker, preset)
            with self.assertRaisesRegex(RuntimeError, "WorkerA: CMYK 输入没有可用 ICC"):
                Renderer(root / "cache-zh", language="zh-Hans")._apply_color_transform(image, current_worker, preset)

    def test_icc_transform_failure_can_localize_to_chinese(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            image = Image.new("RGB", (4, 4))
            current_worker = worker(root)
            preset = current_worker.get_active_preset()

            with patch("printfarm.renderer.ImageCms.profileToProfile", return_value=None):
                with self.assertRaisesRegex(RuntimeError, "ICC 颜色转换失败"):
                    Renderer(root / "cache-zh", language="zh-Hans")._apply_color_transform(image, current_worker, preset)


class RenderCacheKeyTests(TestCase):
    @staticmethod
    def _worker(directory: Path, name: str, rendering_intent: str = "relative_colorimetric") -> WorkerConfig:
        preset = PresetConfig(name="default", rendering_intent=rendering_intent)
        return WorkerConfig(
            name=name,
            directory=directory,
            printer_name=f"Printer-{name}",
            active_preset=preset.name,
            presets={preset.name: preset},
        )

    @staticmethod
    def _sample_image(path: Path) -> None:
        Image.new("RGB", (200, 100), (10, 20, 30)).save(path, dpi=(300, 300))

    def test_identically_configured_workers_share_one_cache(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            image_path = root / "page.png"
            self._sample_image(image_path)
            renderer = RecordingRenderer(root / "cache")
            worker_a_dir = root / "WorkerA"
            worker_b_dir = root / "WorkerB"
            worker_a_dir.mkdir()
            worker_b_dir.mkdir()
            worker_a = self._worker(worker_a_dir, "Alpha")
            worker_b = self._worker(worker_b_dir, "Bravo")

            artifact_a = renderer.ensure_render_cache(TaskItem(file_path=image_path), worker_a)
            artifact_b = renderer.ensure_render_cache(TaskItem(file_path=image_path), worker_b)

            self.assertEqual(artifact_a.cache_dir, artifact_b.cache_dir)
            # The second worker reused the cache instead of rendering again.
            self.assertEqual(len(renderer.color_transform_input_sizes), 1)

    def test_cmyk_fallback_icc_content_change_invalidates_cache(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            image_path = root / "page.png"
            self._sample_image(image_path)
            icc_path = root / "fallback.icc"
            icc_path.write_bytes(b"profile-one")
            current_worker = self._worker(root, "Alpha")

            # Fresh Renderer per call simulates a fresh per-run instance.
            first = RecordingRenderer(root / "cache", cmyk_fallback_icc=str(icc_path)).ensure_render_cache(
                TaskItem(file_path=image_path), current_worker
            )
            icc_path.write_bytes(b"profile-two-different")
            second = RecordingRenderer(root / "cache", cmyk_fallback_icc=str(icc_path)).ensure_render_cache(
                TaskItem(file_path=image_path), current_worker
            )

            self.assertNotEqual(first.cache_dir, second.cache_dir)

    def test_rendering_intent_change_invalidates_cache(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            image_path = root / "page.png"
            self._sample_image(image_path)
            renderer = RecordingRenderer(root / "cache")
            perceptual = self._worker(root, "Alpha", rendering_intent="perceptual")
            relative = self._worker(root, "Alpha", rendering_intent="relative_colorimetric")

            first = renderer.ensure_render_cache(TaskItem(file_path=image_path), perceptual)
            second = renderer.ensure_render_cache(TaskItem(file_path=image_path), relative)

            self.assertNotEqual(first.cache_dir, second.cache_dir)

    def test_unreadable_icc_fingerprint_differs_from_unconfigured(self) -> None:
        # A configured-but-unreadable profile must not collapse to the "" used
        # for an unset profile, or its cache key would collide with "no ICC".
        with TemporaryDirectory() as tmp:
            missing = Path(tmp) / "missing.icc"
            unconfigured = _icc_fingerprint(None)
            broken = _icc_fingerprint(missing)

            self.assertEqual(unconfigured, "")
            self.assertNotEqual(broken, "")
            self.assertNotEqual(broken, unconfigured)

    def test_broken_output_icc_does_not_reuse_no_icc_cache(self) -> None:
        # Render once with no output ICC so a cache exists, then point the same
        # otherwise-identical worker at a missing output ICC. The broken config
        # must surface as an error instead of silently serving the no-ICC cache.
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            image_path = root / "page.png"
            self._sample_image(image_path)
            cache = root / "cache"

            no_icc_worker = self._worker(root, "Alpha")
            Renderer(cache).ensure_render_cache(TaskItem(file_path=image_path), no_icc_worker)

            broken_worker = self._worker(root, "Alpha")
            broken_worker.get_active_preset().output_icc = str(root / "missing.icc")
            with self.assertRaisesRegex(RuntimeError, r"output \(printer\) ICC not found"):
                Renderer(cache).ensure_render_cache(TaskItem(file_path=image_path), broken_worker)


class SourceSignatureTests(TestCase):
    def test_crc_detects_content_change_when_size_and_mtime_are_identical(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "input.bin"
            source.write_bytes(b"A" * 256)
            stat = source.stat()
            os.utime(source, ns=(stat.st_atime_ns, stat.st_mtime_ns))

            # A fresh Renderer per call simulates a fresh per-run instance.
            sig_first = Renderer(root / "cache")._source_signature(source)
            source.write_bytes(b"B" * 256)  # same size, different content
            os.utime(source, ns=(stat.st_atime_ns, stat.st_mtime_ns))  # restore identical mtime
            sig_second = Renderer(root / "cache")._source_signature(source)

            self.assertEqual(sig_first["size"], sig_second["size"])
            self.assertEqual(sig_first["mtime_ns"], sig_second["mtime_ns"])
            self.assertNotEqual(sig_first["crc32"], sig_second["crc32"])

    def test_signature_is_cached_per_renderer_instance(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "input.bin"
            payload = b"hello world" * 10
            source.write_bytes(payload)
            stat = source.stat()
            renderer = Renderer(root / "cache")

            first = renderer._source_signature(source)
            # Replace content but keep size and mtime identical: within one run the
            # memo intentionally serves the cached CRC (source files are stable mid-run).
            source.write_bytes(b"X" * len(payload))
            os.utime(source, ns=(stat.st_atime_ns, stat.st_mtime_ns))
            second = renderer._source_signature(source)

            self.assertEqual(first["crc32"], second["crc32"])
