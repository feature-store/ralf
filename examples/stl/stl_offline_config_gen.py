import json
import os
from glob import glob

from tqdm import tqdm
import numpy as np
import pandas as pd
from absl import app, flags
from ortools.linear_solver import pywraplp
from sktime.performance_metrics.forecasting import mean_squared_scaled_error

FLAGS = flags.FLAGS
flags.DEFINE_string(
    "csv_dir",
    default="./result/offline_1_slide/plan_eval",
    help="CSV directory containg result from offline stl eval and oracles",
)
# TODO(simon): add flags for lp solver constraint
flags.DEFINE_string(
    "output_path",
    default=None,
    help="Output path for the slide size configuration json",
    required=True,
)


def run_lp(df: pd.DataFrame, max_n_fits=None, max_loss=None, objective="min_loss"):
    """Run through mixed integer program to generate the best plan.

    Input:
        df(pd.DataFrame): dataframe containing three columns (key, n_fits, loss)
        max_n_fits(int, None): optionally constraint the total n_fits, default to no constraint.
        max_loss(float, None): optionally constraint the max loss, default to no constraint.
        objective(str): either "min_loss", or "min_fits" given the constraints.
    Output:
        plan(Dict[str, int]): a dictionary mapping key -> optimal n_fits such that loss is minimal.
    """
    assert all(df.columns == ["key", "n_fits", "loss"])
    assert objective in {"min_loss", "min_fits"}

    # Preprocess data into dictionary
    key_n_fits_to_loss = {
        (key, n_fits): loss for _, (key, n_fits, loss) in df.iterrows()
    }

    solver = pywraplp.Solver.CreateSolver("SCIP")

    # For each key, there are many possible choice for n_fits.
    variables = {}
    for (i, j) in key_n_fits_to_loss.keys():
        variables[(i, j)] = solver.IntVar(0, 1, f"x_{i}_{j}")

    # Constraint: For each key, there should only be one n_fits chosen.
    for i in df["key"].unique():
        solver.Add(sum(variables[(i, j)] for j in df["n_fits"].unique()) == 1)

    # Constriant: Every key must have a n_fits chosen, for fairness
    solver.Add(sum(variables.values()) == len(df["key"].unique()))

    # Constraint: Bound n_fits and total loss.
    if max_n_fits:
        solver.Add(
            sum(
                selected_var * n_fits
                for (_key, n_fits), selected_var in variables.items()
            )
            <= max_n_fits
        )
    if max_loss:
        solver.Add(
            sum(
                variables[key_n_fits] * loss
                for key_n_fits, loss in key_n_fits_to_loss.items()
            )
            <= max_loss
        )

    # Objective: Minimize lost/maximize accuracy given cost budget
    solver_objective = solver.Objective()
    for (key, n_fits), loss in key_n_fits_to_loss.items():
        if objective == "min_loss":
            solver_objective.SetCoefficient(variables[(key, n_fits)], loss)
        if objective == "min_fits":
            solver_objective.SetCoefficient(variables[(key, n_fits)], n_fits)
    solver_objective.SetMinimization()

    status = solver.Solve()
    if status != pywraplp.Solver.OPTIMAL:
        if status == pywraplp.Solver.INFEASIBLE:
            raise RuntimeError("Infeasible")
        raise RuntimeError(f"failed with status code {status}")
    optimal_loss = solver_objective.Value()

    plan_output = {k[0]: k[1] for k, v in variables.items() if v.solution_value() > 0}
    return plan_output, optimal_loss


def get_loss_per_key(key: int, csv_dir):
    key_one = glob(f"{csv_dir}/slide_*_key_A4Benchmark-TS{key}.csv")
    assert len(key_one) > 0

    oracle_residual = pd.read_csv(f"{csv_dir}/oracle_key_A4Benchmark-TS{key}.csv")[
        "pred_residual"
    ]

    losses = []
    for path in key_one:
        slide_size = int(
            os.path.basename(path).split("_key_A4")[0].replace("slide_", "")
        )
        df = pd.read_csv(path)
        residual = df["pred_residual"]
        mask = ~np.isnan(residual)
        loss = mean_squared_scaled_error(
            y_true=oracle_residual[mask], y_pred=residual[mask], y_train=df["value"]
        )
        losses.append(
            {
                "slide_size": slide_size,
                "loss": loss,
                "n_fits": df["model_version"].dropna().nunique(),
            }
        )
    return losses


def main(argv):
    raw_data = []
    for key in tqdm(range(1, 101)):
        for entry in get_loss_per_key(key, csv_dir=FLAGS.csv_dir):
            raw_data.append({"key": key, **entry})

    df = pd.DataFrame(raw_data)
    print("loss per n_fits")
    print(df.groupby("n_fits")["loss"].describe())
    print(f"loss per key (sample of 10 out of {len(df)})")
    print(df.groupby("key")["loss"].describe().sample(10))

    print("generating lp config for min_loss")
    lp_result, _loss = run_lp(df[["key", "n_fits", "loss"]], objective="min_loss")

    solution = pd.Series(lp_result).to_frame().reset_index()
    solution.columns = ["solution_key", "solution_n_fits"]
    solution_df = solution.merge(
        df, left_on=["solution_key", "solution_n_fits"], right_on=["key", "n_fits"]
    )
    slide_size_config = (
        solution_df[["key", "slide_size"]]
        .set_index("key", drop=True)
        .to_dict()["slide_size"]
    )

    with open(FLAGS.output_path, "w") as f:
        json.dump(slide_size_config, f)


if __name__ == "__main__":
    app.run(main)
