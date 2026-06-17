from __future__ import annotations

from unittest import TestCase

from printfarm.models import (
    FIT_MODE_ACTUAL,
    FIT_MODE_ACTUAL_100,
    FIT_MODE_FILL,
    FIT_MODE_FIT,
    normalize_fit_mode,
)
from printfarm.spooler import PrinterSpooler


class FitDestinationSizeTests(TestCase):
    """Geometry of PrinterSpooler._fit_destination_size for each page-sizing mode.

    natural_* is the image's true 1:1 device-pixel size; max_* is the target area.
    The result is the centered destination size, so > max means cropped overflow and
    < max means a centered blank margin. Aspect ratio must always be preserved.
    """

    def size(self, mode: str, natural: tuple[int, int], area: tuple[int, int]) -> tuple[int, int]:
        return PrinterSpooler._fit_destination_size(mode, natural[0], natural[1], area[0], area[1])

    def test_actual_keeps_native_size_when_it_fits(self) -> None:
        self.assertEqual(self.size(FIT_MODE_ACTUAL, (100, 50), (200, 200)), (100, 50))

    def test_actual_shrinks_to_fit_when_oversized(self) -> None:
        # 400x200 into 200x200: uniform 0.5 scale keeps aspect and fits.
        self.assertEqual(self.size(FIT_MODE_ACTUAL, (400, 200), (200, 200)), (200, 100))

    def test_actual_100_never_scales_even_when_oversized(self) -> None:
        # Exact 1:1; the overflow is cropped later by centering, not by scaling.
        self.assertEqual(self.size(FIT_MODE_ACTUAL_100, (400, 200), (200, 200)), (400, 200))
        self.assertEqual(self.size(FIT_MODE_ACTUAL_100, (100, 50), (200, 200)), (100, 50))

    def test_fit_scales_up_small_images_to_fit_with_margins(self) -> None:
        # Smallest dimension ratio wins so the whole image stays inside the page.
        self.assertEqual(self.size(FIT_MODE_FIT, (100, 50), (200, 200)), (200, 100))

    def test_fit_scales_down_oversized_images(self) -> None:
        self.assertEqual(self.size(FIT_MODE_FIT, (400, 200), (200, 200)), (200, 100))

    def test_fill_covers_page_and_crops(self) -> None:
        # Largest dimension ratio wins so the page is fully covered (overflow cropped).
        self.assertEqual(self.size(FIT_MODE_FILL, (100, 50), (200, 200)), (400, 200))
        self.assertEqual(self.size(FIT_MODE_FILL, (400, 100), (200, 200)), (800, 200))

    def test_unknown_mode_normalizes_to_actual(self) -> None:
        self.assertEqual(normalize_fit_mode("nonsense"), FIT_MODE_ACTUAL)
        self.assertEqual(normalize_fit_mode(""), FIT_MODE_ACTUAL)
        self.assertEqual(normalize_fit_mode(None), FIT_MODE_ACTUAL)
