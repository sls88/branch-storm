# Branch Storm — workflow orchestrator for Python

Branch Storm is an open‑source library for building **typed, readable, and debuggable**
data pipelines as explicit **branches of operations**. It brings strong typing,
clear logging, and convenient argument routing to ordinary Python code.

> Install: `pip install branch-storm`

---

## Why Branch Storm?

- **Operation‑level typing** – precise type checks (including nested structures) on
  inputs to every operation, not just function signatures.
- **Readable pipeline DSL** – compose pipelines as branches that mirror how data
  actually flows (read → transform → write), without hidden magic.
- **Great logs** – every operation is executed with a visible **execution stack**
  so you can quickly see where and why things failed.
- **Argument plumbing you don’t have to write** – pass results between operations
  explicitly or automatically distribute leftovers when appropriate.
- **Ergonomic parallelism** – run multiple branches in parallel without inventing
  infrastructure each time.
- **Reusable “rw‑instances”** – pass special objects (values, variables, run config)
  through the pipeline without threading them by hand.

---

## Core ideas in 60 seconds

### Operations are explicit

An **Operation** wraps a target you want to execute: a function, a class constructor,
or an instance method. You declare the operation with `CallObject`, then turn it into
an executable step with `Operation`:

```python
from branch_storm import Operation as op, CallObject as obj

def add1(x: int) -> int:
    return x + 1

step = op(obj(add1)(10))   # produces an Operation that will call add1(10) at run time
result = step.run(())      # -> (11, ())  result 11, The remaining arguments are a single tuple that was
                           #                         passed to the run method as input and did not participate
                           #                         in the initialization.
```

You compose operations inside a **Branch**:

```python
from branch_storm import Branch as br

actual = br("my_job")[
    op(obj(add1)(10))
].run()                     # -> 11
```

### Type containers route inputs and validate types

Branch Storm uses two “type containers” to **route** incoming data into an operation
and **validate** its types:

- `MandatoryArgTypeContainer` (alias `m`) – consumes a value and **must** be satisfied,
  otherwise a type error is raised.
- `OptionalArgTypeContainer` (alias `opt`) – consumes a value **if present**; if not,
  the operation’s default is used without error.

Common patterns:
- `m[int]` – consume one value and check it is `int`.
- `m("cfg.bucket")[str]` – consume a string found in rw‑instance `cfg.bucket`.
- `m(2)[int]` – consume the **2nd** value from the input data tuple.
- `m(seq=True)[int]` – consume a **sequence** of `int` until a different type is met.
Some action must be performed on the remaining arguments, otherwise an exception will be thrown.

Anything not consumed by containers is considered **remaining args** for the next step.
You can:
- **distribute** them downstream (`.distribute_input_data`),
- **stop distribution** (`.stop_distribution`), or
- **burn** them (`.burn_rem_args`) if you intentionally want to drop leftovers.

---

## Passing arguments between operations

`CallObject` can target functions, classes (including static/class methods) and instances.
The call can be a chain with attribute access and `[]` lookups:

```python
obj(SomeClass)(init_arg="g").meth1(3).prop1.other(7)["key"]
```

### Mandatory & Optional containers

When you place containers inside the call, Branch Storm will **pull** values from the
incoming data tuple and/or from **rw‑instances** (see below), validate their types
(via `typeguard`), and pass them to the operation:

```python
from typing import Tuple
from branch_storm import Branch as br, Operation as op, CallObject as obj, \
    MandatoryArgTypeContainer as m, OptionalArgTypeContainer as opt

def args_kwargs_func(arg1: list, arg2, arg3: int, arg4: str, arg5: str, arg6: int, *rest,
                     kwarg1: int, kwarg2: str = "7i", kwarg3: str = "", **kws) -> Tuple:
    return arg1, arg2, arg3, arg4, arg5, arg6, rest, kwarg1, kwarg2, kwarg3, kws

operation = op(obj(args_kwargs_func)(
    [None], 5, m[int], "uuu", m[str], 3,
    m[float], m(seq=True)[bool], m[str], m[str], m(seq=True)[int], "x5", m[str],
    kwarg1=9, kwarg3=m(3)[str], kw100=90, kw200=m(4)[str]
))

init_data = (1, "tt", "lll", "pppp", 4.0, True, True, True, "str1", "str2", 1, 2, 3, 4, "F", 13)
actual_result = operation.run(init_data)

expected_data = (
    [None], 5, 1, 'uuu', 'tt', 3,
    (4.0, True, True, True, 'str1', 'str2', 1, 2, 3, 4, 'x5', 'F'),
    9, '7i', 'lll', {'kw100': 90, 'kw200': 'pppp'})

assert actual_result == (expected_data, (13,))
```

Anything **not** consumed becomes **remaining args** and is returned as the second element
of the result tuple unless you choose to distribute or burn them.

---

## Branches

A **Branch** is a processor that executes operations (and nested branches) inside it.
It accepts a name (for logging) and a custom **processor**.

There are two built‑in processors:
- `BrIterativeProcessor` (default) — executes steps in a loop (avoids recursion depth issues).
- `BrRecursiveProcessor` — calls steps recursively; functions do not store state 
but typically limited to ~900 ops per branch.

The processor passed to the root branch once 
will process all operations and nested branches inside it, unless 
changed to nested.

**Execution order:** If a branch is nested in a chain of operations, then when
the queue reaches it, it will be executed first
(all nested operations inside it sequentially) and return the result and any unconsumed arguments.
They can be distributed to other branches or operations.

Options can also be applied to branches.

---

## rw‑instances (create “variables” for your pipeline)

“rw‑instances” are special class instances you provide via `.rw_inst({...})` to share
data across operations without threading arguments through every call.

- Provide a dict of **aliases** → **instances** to a branch or an operation:
  ```python
  .rw_inst({"val": Values(), "var": Variables(), 
            "run_conf": RunConfigurations(), "your": CustomClass()})
  ```
- You can pass these objects to operations using string references inside containers,
  e.g. `m("val.user_id")[int]` or `m("cfg")[JobConfig]`.
- The rw‑inst stack supports **renaming**: pass `{"old_alias": "new_alias"}` to remap.
- And **deletion** via `{"alias": "drop"}` / `{"alias": "delete"}` / `"remove"` / `"del"`.
  (Deleting default classes is prohibited and will raise.)

### Default rw‑instances

The following are available by default (aliases shown):

- **Values** (`"val"`) — for **immutable** data only; fields can be written **once** then read.
  Supported types: `str`, `int`, `float`, `complex`, `range`, `bool`, `bytes`,
  `bytearray`, `memoryview`.
- **Variables** (`"var"`) — for any data; dynamic attributes are created on write.
- **RunConfigurations** (`"run_conf"`) — reserved alias, cannot be renamed/deleted.
  Stores the complete call stack of branches and operations; required for built‑in
  parallelism so a new branch can continue execution with the **previous** stack.

You can write to these classes in two ways:
- **Inside operations** (they are ordinary Python objects).
- With **`.assign(...)`** on an Operation to write its returned tuple into fields. Example:
  ```python
  op(obj(some_func)()).assign("val.first_value", "var.second_value")
  ```

---

## Distributing & burning remaining arguments

Containers “eat” some incoming data. What about the rest? You control it explicitly.

- `.distribute_input_data` — keep passing remaining args to subsequent steps.
- `.stop_distribution` — stop distributing at this point and return accumulated results.
- `.burn_rem_args` — intentionally **drop** the remaining args at this point.

Example:

```python
from typing import Tuple
from branch_storm import Branch as br, Operation as op, CallObject as obj, MandatoryArgTypeContainer as m

def return_1_2_3(): return 1, 2, 3
def pass_one(arg: int) -> int: return arg
def pass_three(a: int, b: int, c: int) -> Tuple[int, int, int]: return a, b, c

actual = br("distribute-demo")[
    obj(return_1_2_3)(),                       # -> (1,2,3)
    op(obj(pass_one)(m[int])).distribute_input_data,
    obj(pass_one)(m[int]),                     # still distributing
    op(obj(pass_one)(m[int])).stop_distribution,
    obj(pass_three)(m[int], m[int], m[int]),   # consumes delayed returns
].run()

assert actual == (1, 2, 3)
```

---

## Starting operations & stopping the chain

Before each step, Branch Storm checks whether the call **needs** inputs (mandatory containers).
If not, it is executed without inputs.

Two options control short‑circuiting and errors:

- **`.end_chain_if(cond)`** — if `cond(incoming_data)` is `True`, the pipeline **stops gracefully**
  for the current branch hierarchy. Internally a `STOP_CONSTANT` is returned and **propagates up**
  through nested branches. You can **absorb** it using `.force_call` on a step that must still run.
- **`.raise_err_if(cond)`** — if `cond(incoming_data)` is `True`, raise immediately and stop the job.

You can also return `STOP_CONSTANT` from your own operation to stop further steps the same way.

---

## Type‑check strategy

Type checks are powered by `typeguard`. By default the library validates **all** elements
of sequences (`.check_type_strategy_all(True)`). For very large inputs you can switch to
first‑element only with `.check_type_strategy_all(False)` to speed things up. When set on a branch,
the setting cascades into nested branches until overridden.

> You can also avoid heavy checks by passing an **empty** container (e.g. `m` / `opt` without a type),
> but it is not recommended because it may let unexpected types through.

---

## Logging options

Use `.hide_log_inf(init_inf: bool = None, all_inf: bool = None)`:

- `init_inf=True` — hide initialization info, keep execution stack.
- `all_inf=True` — hide everything except internal loggers (not recommended).

As with other options, settings cascade to nested branches unless overridden.

---

## Blocking calls (capture) with `register_ops` and `typed_alias`

To keep your pipeline declarations **pure** (no side effects while building the branch),
Branch Storm intercepts calls to selected functions/classes and turns them into **captured**
`CallObject`s. Execution happens **later**, when the branch runs.

### How it works

- Call **`register_ops(...)`** once at startup with the operations (or modules) you want to capture.
  The capture is thread‑safe and integrated with Branch execution; you don’t need to call any begin/end.
- Use **`typed_alias(name, Type)`** to create **attribute proxies** for live objects. These proxies
  are IDE‑friendly (autocomplete, rename) and at runtime are treated as **string paths** like
  `"cfg.table_name"` that can be consumed by containers such as `m("cfg.table_name")[str]`.

#### Example: blocked calls

```python
from branch_storm import Branch as br, Operation as op, CallObject as obj, \
    MandatoryArgTypeContainer as m, OptionalArgTypeContainer as opt, \
    register_ops, typed_alias, Values, Variables, RunConfigurations

# Your functions
def read(table_name: str): return table_name
def transform(arg: str): return f"Table: {arg}"
def write(arg: str) -> None: pass

# Register for capture so these calls don't execute while building the branch
register_ops(read, transform, write)

# Optional: config alias for IDE & safe string paths at runtime
class JobConfig:
    table: str
    def __init__(self, table: str): self.table = table

cfg = typed_alias("cfg", JobConfig)

# Build the pipeline — calls are captured (no side effects here)
actual = br("my_job")[
    read(m(cfg.table)[str]),
    op(transform(m[str])),
    write(m[str])
].rw_inst({"cfg": JobConfig("dim_term")}).run()
```

### Registering by module/package

You can register entire modules or packages and filter by name, kind, or predicate:

```python
import my_ops_module
from branch_storm import register_ops

# register everything public from a module
register_ops(my_ops_module)

# or a package recursively, with filters
register_ops(
    "my_project.ops",                  # module name or module object
    recurse_packages=True,
    include=["*read*", "*write*"],     # glob names to include
    exclude=["*experimental*"],        # glob names to exclude
    predicate=lambda name, obj: name.endswith("_op")  # extra filter
)
```

---

## Parallelism

Two built‑in helpers run branches in threads:
- `parallelize_without_result`
- `parallelize_with_result_return`

They rely on `RunConfigurations` to carry call stacks across threads. If you implement
custom parallelism, use `run_conf.get_renewed_self_instance()` to share a renewed instance
between threads so stacks don’t merge accidentally.

---

## Frequently used API (cheat sheet)

- Building:
  - `Branch(...)[ ... ]` — compose a pipeline.
  - `Operation(CallObject(...))` — create a step.
  - `CallObject(target)(args...)` — target may be function, class, instance, or dotted string.
- Containers:
  - `m[...]`, `m("alias.field")[...]`, `m(pos)[...]`, `m(seq=True)[...]`
  - `opt[...]` with the same forms as `m[...]`.
- rw‑instances & aliases:
  - `.rw_inst({"val": Values(), "var": Variables(), "run_conf": RunConfigurations(), "your": CustomClass(), ...})`
  - `.assign("val.field1", "var.field2")`
  - `typed_alias("cfg", JobConfig)` → use `cfg.table` inside containers.
- Flow control:
  - `.distribute_input_data`, `.stop_distribution`, `.burn_rem_args`
  - `.end_chain_if(pred)`, `.raise_err_if(pred)`, `.force_call`
  - `.check_type_strategy_all(True|False)`
  - `.hide_log_inf(init_inf=..., all_inf=...)`
- Capture:
  - `register_ops(funcs_or_modules...)` to block calls during pipeline construction.

---

## Requirements

- Python ≥ 3.9
- `typeguard` for deep type checks

---
