from exif_gps_mapper.materialisers.materializer import Materializer


class ExerciseMaterializer(Materializer):

    # Class variables as constants
    INDEX = "id"
    SCHEMA = {
        'id': 'int64',
        'transaction-id': 'int64',
        'start-time': 'datetime64[ns]',
        'start-time-utc-offset': 'int32',
        'has-route': 'bool',
        'detailed-sport-info': 'string'
    }

    def add(self, exercise_data: dict):
        # Convert to tuple
        row = tuple(exercise_data[col] for col in self.SCHEMA)

        # Add
        self.rows.append(row)
