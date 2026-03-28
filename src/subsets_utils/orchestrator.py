import importlib.util
import json
import os
import sys
import time
import traceback
import multiprocessing
from datetime import datetime
from pathlib import Path
from typing import Callable

from .tracking import set_current_task, get_assets_by_writer, get_reads_by_task, get_io_records, clear_tracking


def _get_task_id(fn: Callable) -> str:
    """Get unique task ID from function (module.name)."""
    module = fn.__module__
    # Strip 'src.' prefix if present for cleaner IDs
    if module.startswith('src.'):
        module = module[4:]
    return f"{module}.{fn.__name__}"


class DAG:
    def __init__(self, nodes: dict[Callable, list[Callable]]):
        self.nodes = nodes
        self.state = {}
        self._fn_to_id = {}  # Map function to its ID
        self._id_to_fn = {}  # Reverse lookup
        self._dependents = {}  # Reverse graph: node -> list of nodes that depend on it
        self._needs_continuation = False  # Set True if any node returns True

        # Initialize state and reverse lookup for each node
        for fn in nodes:
            task_id = _get_task_id(fn)
            self._fn_to_id[fn] = task_id
            self._id_to_fn[task_id] = fn
            self._dependents[fn] = []
            self.state[task_id] = {
                "id": task_id,
                "status": "pending",
            }

        # Build reverse dependency graph
        for fn, deps in nodes.items():
            for dep in deps:
                self._dependents[dep].append(fn)

    def _topological_order(self) -> list[Callable]:
        """Return functions in dependency order.

        Uses DFS-style ordering to run dependent nodes immediately after their
        dependencies complete. This ensures download→transform pairs run together,
        freeing disk space before the next download.
        """
        in_degree = {fn: len(deps) for fn, deps in self.nodes.items()}
        ready = [fn for fn, deg in in_degree.items() if deg == 0]
        order = []

        while ready:
            fn = ready.pop(0)
            order.append(fn)

            for other_fn, deps in self.nodes.items():
                if fn in deps:
                    in_degree[other_fn] -= 1
                    if in_degree[other_fn] == 0:
                        # Insert at FRONT to run dependent immediately (DFS-style)
                        ready.insert(0, other_fn)

        if len(order) != len(self.nodes):
            raise ValueError("Cycle detected in DAG")

        return order

    def _cleanup_cache(self, fn: Callable):
        """Clean up cached files from upstream nodes if all their dependents are done.

        After a node completes, check each of its dependencies. If ALL nodes that
        depend on that dependency have completed, delete the files it wrote.
        Also cleans up the current node if it has no dependents (leaf node).
        """
        from .config import is_cloud, cache_path

        if not is_cloud():
            return

        # Clean up dependencies if all their dependents are done
        for dep in self.nodes[fn]:
            dep_id = self._fn_to_id[dep]
            dependents = self._dependents[dep]

            all_done = all(
                self.state[self._fn_to_id[d]]["status"] in ("done", "failed", "skipped")
                for d in dependents
            )

            if all_done:
                for asset_path in get_assets_by_writer(dep_id):
                    cache_file = Path(cache_path(asset_path))
                    if cache_file.exists():
                        cache_file.unlink()
                        print(f"[DAG] Cleaned up cache: {asset_path}")

        # Clean up current node if it has no dependents (leaf node)
        if not self._dependents[fn]:
            fn_id = self._fn_to_id[fn]
            for asset_path in get_assets_by_writer(fn_id):
                cache_file = Path(cache_path(asset_path))
                if cache_file.exists():
                    cache_file.unlink()
                    print(f"[DAG] Cleaned up cache (leaf): {asset_path}")

    def _run_task(self, fn: Callable, isolate: bool = False) -> dict:
        """Run a single task, optionally in subprocess for memory isolation."""
        task_id = self._fn_to_id[fn]
        task_state = self.state[task_id]
        task_state["status"] = "running"
        task_state["started_at"] = datetime.now().isoformat()

        if isolate:
            return self._run_in_subprocess(fn, task_state)
        else:
            return self._run_inline(fn, task_state)

    def _run_inline(self, fn: Callable, task_state: dict) -> dict:
        """Run task in current process."""
        # Set context for IO tracking
        set_current_task(task_state["id"])

        try:
            result = fn()
            task_state["status"] = "done"
            # If node returns True, it signals more work remains (continuation needed)
            if result is True:
                task_state["needs_continuation"] = True
                self._needs_continuation = True
        except Exception as e:
            task_state["status"] = "failed"
            task_state["error"] = str(e)
            task_state["traceback"] = traceback.format_exc()
        finally:
            task_state["finished_at"] = datetime.now().isoformat()
            started = datetime.fromisoformat(task_state["started_at"])
            finished = datetime.fromisoformat(task_state["finished_at"])
            task_state["duration_s"] = (finished - started).total_seconds()
            set_current_task(None)

        return task_state

    def _run_in_subprocess(self, fn: Callable, task_state: dict) -> dict:
        """Run task in subprocess for memory isolation."""
        def worker(fn, task_id, queue):
            # Re-setup context in subprocess
            from .tracking import set_current_task
            set_current_task(task_id)

            try:
                result = fn()
                msg = {"status": "done"}
                if result is True:
                    msg["needs_continuation"] = True
                queue.put(msg)
            except Exception as e:
                queue.put({
                    "status": "failed",
                    "error": str(e),
                    "traceback": traceback.format_exc(),
                })

        queue = multiprocessing.Queue()
        proc = multiprocessing.Process(target=worker, args=(fn, task_state["id"], queue))
        proc.start()
        proc.join()

        result = queue.get()
        task_state.update(result)
        if result.get("needs_continuation"):
            self._needs_continuation = True
        task_state["finished_at"] = datetime.now().isoformat()
        started = datetime.fromisoformat(task_state["started_at"])
        finished = datetime.fromisoformat(task_state["finished_at"])
        task_state["duration_s"] = (finished - started).total_seconds()

        return task_state

    def run(self, isolate: bool = False, targets: list[str] | None = None):
        """Execute all tasks in dependency order.

        Args:
            isolate: Run each task in subprocess for memory isolation
            targets: Optional list of node names to run (assumes deps already ran)

        Env vars:
            DAG_TARGET: Comma-separated node names to run (overrides targets arg)
            DAG_ON_FAILURE: "crash" (default) or "continue"
            DAG_MAX_RUN_SECONDS: Max wall-clock seconds before stopping and requesting
                continuation. Unset or 0 = no limit.

        Exit behavior:
            - On failure with crash mode: raises immediately
            - On failure with continue mode: raises after all nodes complete
            - On success with continuation needed: sys.exit(2) for workflow retrigger
            - On success: returns normally (exit 0)
        """
        # Reset tracking state for fresh run
        clear_tracking()

        on_failure = os.environ.get("DAG_ON_FAILURE", "crash")
        max_run_seconds = float(os.environ.get("DAG_MAX_RUN_SECONDS", str(5.5 * 60 * 60)))
        run_start_time = time.time()

        # Env var overrides targets arg
        env_targets = os.environ.get("DAG_TARGET")
        if env_targets:
            targets = [t.strip() for t in env_targets.split(",")]

        order = self._topological_order()

        # Filter to targets if specified
        if targets:
            target_set = set(targets)
            order = [fn for fn in order if self._fn_to_id[fn].split(".")[-2] in target_set]
            if not order:
                # Try matching full ID or function name
                order = [fn for fn in self._topological_order()
                         if self._fn_to_id[fn] in target_set or fn.__name__ in target_set]
            if not order:
                print(f"[DAG] No nodes matched targets: {targets}")
                print(f"[DAG] Available: {[self._fn_to_id[fn] for fn in self.nodes]}")
                return self

        first_failure = None

        for fn in order:
            task_id = self._fn_to_id[fn]
            deps = self.nodes[fn]

            # Check time budget before starting a new node
            if max_run_seconds > 0:
                elapsed = time.time() - run_start_time
                if elapsed >= max_run_seconds:
                    print(f"[DAG] Time budget exhausted ({elapsed/3600:.1f}h) - stopping before {task_id}")
                    self._needs_continuation = True
                    break

            # Check deps succeeded
            for dep in deps:
                dep_id = self._fn_to_id[dep]
                if self.state[dep_id]["status"] != "done":
                    self.state[task_id]["status"] = "skipped"
                    self.state[task_id]["error"] = f"Dependency {dep_id} failed"
                    continue

            print(f"[DAG] Running {task_id}...")
            result = self._run_task(fn, isolate=isolate)
            self.save_state()  # Implicit checkpoint after each node
            self._cleanup_cache(fn)  # Free disk space from completed upstream nodes

            if result["status"] == "done":
                continuation_msg = " (needs continuation)" if result.get("needs_continuation") else ""
                print(f"[DAG] {task_id} done ({result['duration_s']:.1f}s){continuation_msg}")
            else:
                print(f"[DAG] {task_id} failed: {result.get('error', 'unknown')}")
                if first_failure is None:
                    first_failure = result
                if on_failure == "crash":
                    # Raise immediately
                    raise RuntimeError(f"[DAG] {task_id} failed: {result.get('error', 'unknown')}")

        # After all nodes complete, check for deferred failures (continue mode)
        if first_failure is not None:
            task_id = first_failure["id"]
            raise RuntimeError(f"[DAG] {task_id} failed: {first_failure.get('error', 'unknown')}")

        # All nodes succeeded - check if continuation needed
        if self._needs_continuation:
            print("[DAG] Continuation needed - exiting with code 2 for retrigger")
            sys.exit(2)

        return self

    def to_json(self) -> dict:
        """Export DAG structure and execution state."""
        # Enrich node state with categorized reads/writes
        nodes_with_io = []
        for node_state in self.state.values():
            task_id = node_state["id"]
            writes = get_assets_by_writer(task_id)
            reads = get_reads_by_task(task_id)

            # Categorize writes by path prefix
            raw_writes = [w for w in writes if w.startswith("raw/") or "/raw/" in w]
            materializations = [w.replace("subsets/", "") for w in writes if w.startswith("subsets/")]

            # Categorize reads by path prefix
            raw_reads = [r for r in reads if r.startswith("raw/") or "/raw/" in r]
            subsets_reads = [r.replace("subsets/", "") for r in reads if r.startswith("subsets/")]

            nodes_with_io.append({
                **node_state,
                "raw_reads": raw_reads,
                "raw_writes": raw_writes,
                "subsets_reads": subsets_reads,
                "materializations": materializations,
            })

        return {
            "nodes": nodes_with_io,
            "edges": [
                {"from": self._fn_to_id[dep], "to": self._fn_to_id[fn]}
                for fn, deps in self.nodes.items()
                for dep in deps
            ],
            "status": self._overall_status(),
            "total_duration_s": sum(
                n.get("duration_s", 0) for n in self.state.values()
            ),
            "io_trace": get_io_records(),  # Full IO trace with stacks for debugging
        }

    def _overall_status(self) -> str:
        statuses = [n["status"] for n in self.state.values()]
        if "failed" in statuses:
            return "failed"
        if "running" in statuses:
            return "running"
        if all(s == "done" for s in statuses):
            return "done"
        return "pending"

    def save_state(self):
        """Save execution state to LOG_DIR. Called after each node, can also be called explicitly."""
        log_dir = os.environ.get('LOG_DIR')
        if not log_dir:
            return  # No LOG_DIR = local dev without runner, skip
        path = Path(log_dir) / "dag.json"
        path.write_text(json.dumps(self.to_json(), indent=2))


def load_nodes(nodes_dir: Path | str | None = None) -> DAG:
    if nodes_dir is None:
        nodes_dir = Path.cwd() / "src" / "nodes"
    elif isinstance(nodes_dir, str):
        nodes_dir = Path(nodes_dir)

    print(f"Loading nodes from: {nodes_dir}")

    all_nodes: dict[Callable, list[Callable]] = {}

    if not nodes_dir.exists():
        print(f"Warning: nodes directory not found: {nodes_dir}")
        return DAG(all_nodes)

    node_files = sorted(nodes_dir.glob("*.py"))

    for node_file in node_files:
        if node_file.name.startswith("_"):
            continue

        module_name = f"nodes.{node_file.stem}"

        try:
            # Check if module was already loaded (e.g., imported by another node)
            if module_name in sys.modules:
                module = sys.modules[module_name]
            else:
                # Load module dynamically
                spec = importlib.util.spec_from_file_location(module_name, node_file)
                if spec is None or spec.loader is None:
                    print(f"Warning: Could not load spec for {node_file}")
                    continue

                module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = module
                spec.loader.exec_module(module)

            # Extract NODES dict if present
            if hasattr(module, "NODES"):
                nodes_dict = getattr(module, "NODES")
                if isinstance(nodes_dict, dict):
                    # Format: {fn: [deps]} - direct pass to DAG
                    for fn, deps in nodes_dict.items():
                        all_nodes[fn] = deps

        except Exception as e:
            print(f"Error loading {node_file.name}: {e}")
            raise

    print(f"Loaded {len(all_nodes)} nodes")
    return DAG(all_nodes)
