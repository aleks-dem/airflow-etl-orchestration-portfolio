import sys
from pathlib import Path

from airflow.models import DagBag

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _load_dag_bag() -> DagBag:
    dag_folder = str(PROJECT_ROOT / "dags")
    return DagBag(dag_folder=dag_folder, include_examples=False)


def test_dag_imports_have_no_errors():
    dag_bag = _load_dag_bag()
    assert not dag_bag.import_errors, f"Import errors: {dag_bag.import_errors}"


def test_all_dags_have_tags_and_owner():
    dag_bag = _load_dag_bag()
    assert dag_bag.dags, "No DAGs were loaded"
    for dag in dag_bag.dags.values():
        assert dag.tags, f"DAG {dag.dag_id} should have at least one tag"
        assert dag.owner, f"DAG {dag.dag_id} should define owner"


def test_demo_prefix_for_all_dag_ids():
    dag_bag = _load_dag_bag()
    assert dag_bag.dags, "No DAGs were loaded"
    for dag_id in dag_bag.dags:
        assert dag_id.startswith("demo_"), f"DAG id should start with demo_: {dag_id}"
