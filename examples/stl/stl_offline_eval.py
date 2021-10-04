import argparse
import bisect

import numpy as np
import pandas as pd
from statsmodels.tsa.seasonal import STL


def train(data, window_size, seasonality):
    window = data[-window_size:]
    values = [r["value"] for r in window]
    stl_result = STL(values, period=seasonality, robust=True).fit()
    timestamp = data[-1]["timestamp"]
    return {
        "timestamp": timestamp,
        "trend": stl_result.trend[-1],
        "seasonality": list(stl_result.seasonal[-(seasonality + 1) : -1]),
        "stl_result": stl_result,
    }


def predict(event, model):
    """
    Calculate predicted residual and staleness (compared to last model timestamp) given event, model
    """
    # TODO: BE CAREFUL - changes based off timestamp units
    staleness = int(event["timestamp"] - model["timestamp"])
    last_trend = model["trend"]
    seasonal = model["seasonality"][staleness % len(model["seasonality"])]

    # calculate residual
    residual = event["value"] - last_trend - seasonal
    return residual, last_trend, seasonal, staleness


SEASONALITY = 24 * 7


def offline_eval(yahoo_csv_path, plan_json_path):
    df = pd.read_csv(yahoo_csv_path)
    df["timestamp"] = list(range(len(df)))

    # Headers
    # processing_time  window_start_seq_id  window_end_seq_id  key
    plan_df = pd.read_json(plan_json_path)

    # Given our model versions from offline plan, run training on corresponding
    # events.
    offline_stl = {}
    for _, row in plan_df.iterrows():
        records = df.iloc[row.window_start_seq_id : row.window_end_seq_id + 1].to_dict(
            orient="records"
        )

        # The yahoo dataset seasonaly can be 12hr, daily, and weekly.
        # Each record is an hourly record. Here we chose weekly seasonality.
        trained = train(records, window_size=len(records), seasonality=SEASONALITY)
        offline_stl[row.processing_time] = trained

    # Assign the trained model with every events in the source file.
    def find_freshest_model_version(event_time, model_versions):
        model_loc = bisect.bisect_left(model_versions, event_time) - 1
        if model_loc < 0:  # This event time is even before any model trained.
            return None
        return model_versions[model_loc]

    df["model_version"] = [
        find_freshest_model_version(et, plan_df["processing_time"])
        for et in df["timestamp"]
    ]

    # Run prediction!
    predicted = []
    for _, row in df.iterrows():
        model_version = row["model_version"]
        if np.isnan(model_version):
            predicted.append(
                {
                    "pred_residual": None,
                    "pred_trend": None,
                    "pred_seasonality": None,
                    "pred_staleness": None,
                }
            )
            continue
        result = predict(row, offline_stl[model_version])
        predicted.append(
            {
                "pred_residual": result[0],
                "pred_trend": result[1],
                "pred_seasonality": result[2],
                "pred_staleness": result[3],
            }
        )
    add_df = pd.DataFrame(predicted)
    for new_col in add_df.columns:
        df[new_col] = add_df[new_col]
    return df


def offline_oracle(yahoo_csv_path):
    df = pd.read_csv(yahoo_csv_path)
    df["timestamp"] = list(range(len(df)))
    df["model_version"] = "oracle"

    records = df.to_dict(orient="records")
    oracle_model = train(records, len(records), SEASONALITY)
    df["pred_residual"] = oracle_model["stl_result"].resid
    df["pred_trend"] = oracle_model["stl_result"].trend
    df["pred_seasonality"] = oracle_model["stl_result"].seasonal
    df["pred_staleness"] = 0

    return df


def run_exp(csv_path, plan_path, output_path, run_oracle=False):
    if run_oracle:
        df = offline_oracle(csv_path)
    else:
        df = offline_eval(csv_path, plan_path, run_oracle)
    df.to_csv(output_path, index=None)


def main():
    parser = argparse.ArgumentParser(description="Specify experiment config")
    parser.add_argument("--offline-yahoo-csv-path", type=str)
    parser.add_argument("--offline-plan-path", type=str)
    parser.add_argument("--output-path", type=str)
    parser.add_argument("--offline-run-oracle", type=bool, default=False)
    args = parser.parse_args()

    assert args.offline_yahoo_csv_path
    if not args.offline_run_oracle:
        assert args.offline_plan_path

    run_exp(
        csv_path=args.offline_yahoo_csv_path,
        plan_path=args.offline_plan_path,
        output_path=args.output_path,
        run_oracle=args.offline_run_oracle,
    )


if __name__ == "__main__":
    main()
