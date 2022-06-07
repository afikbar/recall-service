from typing import List, Tuple
from flask import Flask, request
import numpy as np

from dals import PsqlDal, KafkaDal

app = Flask(__name__)
psql_dal = PsqlDal()
kafka_dal = KafkaDal()


def _calc_recall(data: List[Tuple[int, int]]) -> float:
    x = np.array(data)
    TP = np.sum(np.logical_and(x[:, 0] == 1, x[:, 1] == 1))
    FN = np.sum(np.logical_and(x[:, 0] == 0, x[:, 1] == 1))

    recall = TP / (TP + FN)
    return recall


@app.route('/')
def hello():
    return "Welcome to the prediction service!"


@app.route('/v1/recall/version/<int:version_id>',  methods=['GET'])
def report_recall(version_id: int):
    segment_id = request.args.get('segment_id', None)
    version_segment_str = f"version {version_id}"
    status_code = 200
    try:
        if segment_id is not None:
            version_segment_str += f" and segment {segment_id}"
        data = psql_dal.get_predicitions_and_actuals(version_id, segment_id)

        if len(data) == 0:
            status_code = 404
            msg = f"No data found for {version_segment_str}"
        else:
            recall = _calc_recall(data)
            success = kafka_dal.publish_recall(version_id, segment_id, recall)

            if success is True:
                msg = f"success calc recall for {version_segment_str}"
            else:
                status_code = 500
                msg = f"failed to calc recall for {version_segment_str}: Couldn't publish to kafka"
    except Exception as e:
        status_code = 500
        msg = f"failed to calc recall for {version_segment_str}: {e}"

    finally:
        return {"Message": msg}, status_code


if __name__ == '__main__':
    app.run(host="localhost", port=8080)
    psql_dal.close()
    kafka_dal.close()
