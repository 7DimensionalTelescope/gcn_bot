"""
TileManager
===========
Wraps the ``supy`` Tiles library and provides a uniform interface for
computing 7DT tile coverage of GCN event error regions.

If ``supy`` cannot be imported ``TileManager.available`` returns ``False``
and all methods return safe default values (``None`` / empty list) so the
rest of the pipeline keeps running without tile information.

A fresh ``Tiles`` instance is created per computation to work around the
upstream caching issue (``if not self.tbl_RIS:`` raises ``ValueError`` on
an astropy Table on the second call).

DBTileChecker
=============
Optional feature (enabled via ``TURN_ON_DB_TILE_CHECK = true`` in settings.toml).
Queries the 7DT GWPortal database to report how many of the required tiles
already have reference images in the DB (prerequisite for image subtraction).
Requires env vars: GWPORTAL_BASE_URL, GWPORTAL_API_KEY.

When the DB client is unavailable, ``get_slack_blocks`` falls back to
displaying only the tile-count summary without DB status.
"""

import logging
import os
import tempfile
from io import BytesIO
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    from supy.supy.tiles.tiles import Tiles as _Tiles
    _supy_tiles_available = True
    logger.info("supy Tiles library loaded successfully")
except ImportError as _exc:
    _supy_tiles_available = False
    _Tiles = None
    logger.warning(f"supy Tiles not available — tile coverage features disabled. ({_exc})")

# ---------------------------------------------------------------------------
# DB tile checker — wraps GWPortalClient
# ---------------------------------------------------------------------------


class DBTileChecker:
    """
    Checks how many required tile IDs have reference images in the 7DT DB.

    Queries ``/api/tiles/`` filtered by tile name. A tile that appears in the
    response has been observed and therefore has reference images available
    for image subtraction.

    ``gwportal_client`` must be importable (e.g. on PYTHONPATH); if it is not,
    ``available`` is ``False`` and ``count_existing`` returns ``None`` silently.

    ``base_url`` / ``api_key`` come from settings.toml (GWPORTAL_BASE_URL /
    GWPORTAL_API_KEY); when blank the client falls back to the like-named
    environment variables.

    Instantiation attempts one connection test; if it fails ``available`` is
    also ``False``.
    """

    def __init__(self, base_url: Optional[str] = None, api_key: Optional[str] = None) -> None:
        self._client = None
        try:
            from api.gwportal_client import GWPortalClient  # noqa: PLC0415
            self._client = GWPortalClient(base_url=base_url or None, api_key=api_key or None)
        except Exception as exc:
            logger.warning(
                f"DBTileChecker: GWPortalClient unavailable — DB check disabled. ({exc})"
            )

    @property
    def available(self) -> bool:
        return self._client is not None

    def count_existing(self, tile_ids: List[str]) -> Optional[int]:
        """
        Return how many of ``tile_ids`` exist in the GWPortal tile database.

        Parameters
        ----------
        tile_ids : list[str]
            Tile names to look up (e.g. ``["T12345", "T12346"]``).

        Returns
        -------
        int | None
            Number of matching tiles found in the DB, or ``None`` on error.
        """
        if not self.available or not tile_ids:
            return None
        try:
            response = self._client.query_tiles(
                tile_name=",".join(str(t) for t in tile_ids),
                page_size=len(tile_ids),
            )
            return int(response.get("count", 0))
        except Exception as exc:
            logger.error(f"DBTileChecker: tile query failed: {exc}")
            return None


class TileManager:
    """
    7DT tile coverage analysis for GCN event error regions.

    Parameters
    ----------
    tile_path : str | None
        Path to the tile info file. Uses the supy default when ``None``.
    fraction_overlap_lower : float
        Minimum overlap fraction for a tile to be counted (default 0.1).
    match_tolerance_minutes : float
        Arcminute threshold for the ``is_within_boundary`` flag (default 4).
    point_match_threshold : float
        Error radii (degrees) below this value use point-based matching
        (aperture=0) instead of aperture matching.  7DT tiles are ~1.6°×0.9°;
        the minimum error that produces ≥10% overlap is ~0.21°, so 0.3° gives
        a comfortable margin.  Default 0.3°.
    enable_db_check : bool
        When ``True``, query the GWPortal DB after computing tile coverage to
        report how many required tiles already have reference images
        (default False).
    gwportal_base_url : str | None
        GWPortal API base URL for the DB check. When ``None``/blank the client
        falls back to the GWPORTAL_BASE_URL env var. Only used when
        ``enable_db_check`` is ``True``.
    gwportal_api_key : str | None
        GWPortal API key for the DB check. When ``None``/blank the client falls
        back to the GWPORTAL_API_KEY env var. Only used when ``enable_db_check``
        is ``True``.
    """

    def __init__(
        self,
        tile_path: Optional[str] = None,
        fraction_overlap_lower: float = 0.1,
        match_tolerance_minutes: float = 4.0,
        point_match_threshold: float = 0.3,
        enable_db_check: bool = False,
        label: str = "7DT",
        gwportal_base_url: Optional[str] = None,
        gwportal_api_key: Optional[str] = None,
    ) -> None:
        self._tile_path = tile_path
        self.fraction_overlap_lower = fraction_overlap_lower
        self.match_tolerance_minutes = match_tolerance_minutes
        self.point_match_threshold = point_match_threshold
        self.label = label

        # Probe availability once at init
        self._available = False
        if _supy_tiles_available:
            try:
                _Tiles(tile_path=tile_path)
                self._available = True
                logger.info("TileManager: supy Tiles available")
            except Exception as exc:
                logger.warning(f"TileManager: supy Tiles init probe failed: {exc}")

        # Optional DB reference-image checker
        self._db_checker: Optional[DBTileChecker] = (
            DBTileChecker(base_url=gwportal_base_url, api_key=gwportal_api_key)
            if enable_db_check else None
        )

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def available(self) -> bool:
        """``True`` when the supy Tiles library is ready."""
        return self._available

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_tile_info(
        self,
        ra: float,
        dec: float,
        error: Optional[float],
    ) -> Optional[Dict[str, Any]]:
        """
        Compute tile coverage for a single target.

        Parameters
        ----------
        ra, dec : float
            Target coordinates in degrees (J2000).
        error : float | None
            Error radius in degrees. Returns ``None`` when 0 or absent —
            tile computation is meaningless for a point source.

        Returns
        -------
        dict | None
            ``{"n_tiles": int, "tile_ids": list[str]}``
            or ``None`` if unavailable, or when error is 0 / None.
        """
        if not self.available or not error:
            return None

        try:
            tiles = self._make_tiles()
            if tiles is None:
                return None

            # Tiles are ~1.6°×0.9°; circles smaller than ~0.21° produce <10%
            # overlap, so fall back to point matching for small error circles.
            aperture = 0 if error < self.point_match_threshold else error

            result = tiles.find_overlapping_tiles(
                ra=ra,
                dec=dec,
                aperture=aperture,
                fraction_overlap_lower=self.fraction_overlap_lower,
                match_tolerance_minutes=self.match_tolerance_minutes,
                visualize=False,
            )

            # find_overlapping_tiles returns a 3-tuple when no tiles match
            if isinstance(result, tuple):
                return {"n_tiles": 0, "tile_ids": [], "db_count": None}

            tile_table = tiles.tile_table
            if tile_table is None or len(tile_table) == 0:
                return {"n_tiles": 0, "tile_ids": [], "db_count": None}

            tile_ids = list(tile_table["id"])
            db_count = (
                self._db_checker.count_existing(tile_ids)
                if self._db_checker is not None
                else None
            )
            return {"n_tiles": len(tile_ids), "tile_ids": tile_ids, "db_count": db_count}

        except Exception as exc:
            logger.error(f"Tile info computation failed for RA={ra}, DEC={dec}: {exc}")
            return None

    def generate_plot(
        self,
        ra: float,
        dec: float,
        error: Optional[float],
        title: Optional[str] = None,
    ) -> Optional[BytesIO]:
        """
        Generate a tile coverage plot and return it as a ``BytesIO`` PNG buffer.

        Returns ``None`` if unavailable, error is 0 / None, there are no
        matching tiles, or plotting fails.
        """
        if not self.available or not error:
            return None

        try:
            tiles = self._make_tiles()
            if tiles is None:
                return None

            aperture = 0 if error < self.point_match_threshold else error

            with tempfile.TemporaryDirectory() as tmpdir:
                fig_path = tiles.find_overlapping_tiles(
                    ra=ra,
                    dec=dec,
                    aperture=aperture,
                    fraction_overlap_lower=self.fraction_overlap_lower,
                    match_tolerance_minutes=self.match_tolerance_minutes,
                    visualize=True,
                    visualize_savepath=tmpdir,
                    show=False,
                    visualize_ncols=1,
                    title=title
                )

                # 3-tuple return means no tiles found → nothing to plot
                if isinstance(fig_path, tuple) or fig_path is None:
                    logger.info(f"No tile plot generated for RA={ra}, DEC={dec}: no matching tiles")
                    return None

                if not os.path.exists(fig_path):
                    logger.warning(f"Tile plot file not found at {fig_path}")
                    return None

                with open(fig_path, "rb") as fh:
                    buf = BytesIO(fh.read())

            buf.seek(0)
            return buf

        except Exception as exc:
            logger.error(f"Tile plot generation failed for RA={ra}, DEC={dec}: {exc}")
            return None

    def get_slack_blocks(
        self,
        tile_result: Optional[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Build a Slack block element summarising tile coverage.

        Parameters
        ----------
        tile_result : dict | None
            As returned by :meth:`get_tile_info`.

        Returns
        -------
        list[dict]
            Zero or more Slack block dicts ready to append to a message.
        """
        if tile_result is None:
            return []

        n = tile_result.get("n_tiles", 0)
        db_count = tile_result.get("db_count")  # None when DB check is disabled

        label = self.label
        tile_line = f"*{n} tile{'s' if n != 1 else ''}* needed to cover the error region"
        if n == 0:
            text = f"*[{label} Tiles]* \n *No tiles* cover the error region"
        elif db_count is None:
            # DB check disabled or unavailable — show tile count only
            text = f"*[{label} Tiles]* \n {tile_line}"
        elif db_count >= n:
            text = f"*[{label} Tiles]* \n {tile_line}\n ✅ All reference images exist. ({n}/{n})"
        elif db_count > 0:
            text = f"*[{label} Tiles]* \n {tile_line}\n ⚠️ Some reference images exist. {db_count} / {n}"
        else:
            text = f"*[{label} Tiles]* \n {tile_line}\n ❌ Reference images do not exist. Additional observation is required."

        return [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": text},
            }
        ]

    # ------------------------------------------------------------------
    # Private
    # ------------------------------------------------------------------

    def _make_tiles(self) -> Optional[Any]:
        """Return a fresh ``Tiles`` instance for each computation."""
        try:
            return _Tiles(tile_path=self._tile_path)
        except Exception as exc:
            logger.error(f"TileManager: failed to create Tiles instance: {exc}")
            return None
