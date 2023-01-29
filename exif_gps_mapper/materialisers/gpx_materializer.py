import gpxpy

from exif_gps_mapper.materialisers.materializer import Materializer


class GpxMaterializer(Materializer):

    # Class variables as constants
    INDEX = "point_time"
    SCHEMA = {
        'exercise_id': 'int64',
        'latitude': 'float64',
        'longitude': 'float64',
        'point_time': 'datetime64[ns]'
    }

    def add(self, gpx_data: str, exercise_id: int):

        if gpx_data:

            assert exercise_id is not None, "The required foreign key exercise_id was passed in as NULL."

            # Parse to GPX object
            gpx = gpxpy.parse(gpx_data)

            # Generate rows
            for track in gpx.tracks:
                for segment in track.segments:
                    for p in segment.points:
                        # Cut out microseconds
                        time_trunc = p.time.replace(microsecond=0, tzinfo=None)

                        # Append as tuple
                        self.rows.append(
                            (exercise_id, p.latitude, p.longitude, time_trunc)
                        )
