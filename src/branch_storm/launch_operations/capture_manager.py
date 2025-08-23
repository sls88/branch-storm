import fnmatch
import importlib
import inspect
import pkgutil
import threading
from collections import defaultdict
from contextvars import ContextVar
from types import FrameType, ModuleType
from typing import Any, Callable, Dict, Set, Type, Union, Optional, Iterable, \
    Tuple, TypeVar, cast

from ..operation import CallObject

_OP_REGISTRY: Set[int] = set()

def is_registered(obj: Any) -> bool:
    return id(obj) in _OP_REGISTRY

def unregister_ops(*objs: Any) -> None:
    for obj in objs:
        try:
            _OP_REGISTRY.discard(id(obj))
        except Exception:
            pass

def clear_registry() -> None:
    _OP_REGISTRY.clear()

def operation(obj: Union[Callable, Type]) -> Union[Callable, Type]:
    """Decorator: mark a function/class as interceptable operation."""
    _OP_REGISTRY.add(id(obj))
    return obj

def _looks_like_op(obj: Any,
                   include_callables_with_dunder_call: bool) -> bool:
    if inspect.isfunction(obj) or inspect.isclass(obj):
        return True
    if include_callables_with_dunder_call and callable(obj):
        return True
    return False

def _iter_module_objects(
        mod: ModuleType,
        *,
        public_only: bool,
        only_defined_in_owner: bool,
        include_callables_with_dunder_call: bool,
        include_globs: Optional[Iterable[str]],
        exclude_globs: Optional[Iterable[str]],
        predicate: Optional[Callable[[str, Any], bool]],
) -> Iterable[Tuple[str, Any]]:
    modname = getattr(mod, "__name__", None)
    for name, obj in vars(mod).items():
        if public_only and name.startswith("_"):
            continue
        if include_globs and not any(fnmatch.fnmatch(name, pat)
                                     for pat in include_globs):
            continue
        if exclude_globs and any(fnmatch.fnmatch(name, pat)
                                 for pat in exclude_globs):
            continue
        if only_defined_in_owner and getattr(obj, "__module__", None) != modname:
            continue
        if not _looks_like_op(obj, include_callables_with_dunder_call):
            continue
        if predicate and not predicate(name, obj):
            continue
        yield name, obj

def _walk_package_modules(root_mod: ModuleType) -> Iterable[ModuleType]:
    if not hasattr(root_mod, "__path__"):
        return
    for pkg in pkgutil.walk_packages(root_mod.__path__, root_mod.__name__ + "."):
        try:
            yield importlib.import_module(pkg.name)
        except Exception:
            continue

def _register_from_module(
        mod: ModuleType,
        *,
        recurse_packages: bool,
        public_only: bool,
        only_defined_in_owner: bool,
        include_callables_with_dunder_call: bool,
        include_globs: Optional[Iterable[str]],
        exclude_globs: Optional[Iterable[str]],
        predicate: Optional[Callable[[str, Any], bool]],
) -> None:
    for _, obj in _iter_module_objects(
        mod,
        public_only=public_only,
        only_defined_in_owner=only_defined_in_owner,
        include_callables_with_dunder_call=include_callables_with_dunder_call,
        include_globs=include_globs,
        exclude_globs=exclude_globs,
        predicate=predicate,
    ):
        _OP_REGISTRY.add(id(obj))

    if recurse_packages and hasattr(mod, "__path__"):
        for submod in _walk_package_modules(mod):
            for _, obj in _iter_module_objects(
                submod,
                public_only=public_only,
                only_defined_in_owner=only_defined_in_owner,
                include_callables_with_dunder_call=include_callables_with_dunder_call,
                include_globs=include_globs,
                exclude_globs=exclude_globs,
                predicate=predicate,
            ):
                _OP_REGISTRY.add(id(obj))

def register_ops(
    *objs: Union[Callable, Type, ModuleType, str, Iterable[Any]],
    recurse_packages: bool = False,
    public_only: bool = True,
    only_defined_in_owner: bool = True,
    include_callables_with_dunder_call: bool = False,
    include: Optional[Iterable[str]] = None,
    exclude: Optional[Iterable[str]] = None,
    predicate: Optional[Callable[[str, Any], bool]] = None,
) -> None:
    """
    Register pipeline operations by identity.

    Args:
        *objs: Functions/classes to register directly, modules or module names
               ('pkg.mod'), iterables with the same. You can mix types.
        recurse_packages: If True and a package is passed, walk all submodules.
        public_only: Skip names starting with underscore when scanning modules.
        only_defined_in_owner: Register only objects whose __module__ equals the module being scanned
                               (avoids registering re-exports).
        include_callables_with_dunder_call: Treat arbitrary callables (instances with __call__)
                                            as ops when scanning modules.
        include: Optional glob patterns (['foo_*', 'Bar*']) to whitelist names.
        exclude: Optional glob patterns to blacklist names.
        predicate: Optional callable (name, obj) -> bool for extra filtering.
    """
    def _consume(x: Any) -> None:
        if x is None:
            return
        if isinstance(x, (list, tuple, set)):
            for y in x:
                _consume(y)
            return
        if isinstance(x, str):
            try:
                mod = importlib.import_module(x)
            except Exception:
                return
            _register_from_module(
                mod,
                recurse_packages=recurse_packages,
                public_only=public_only,
                only_defined_in_owner=only_defined_in_owner,
                include_callables_with_dunder_call=include_callables_with_dunder_call,
                include_globs=include,
                exclude_globs=exclude,
                predicate=predicate,
            )
            return
        if isinstance(x, ModuleType):
            _register_from_module(
                x,
                recurse_packages=recurse_packages,
                public_only=public_only,
                only_defined_in_owner=only_defined_in_owner,
                include_callables_with_dunder_call=include_callables_with_dunder_call,
                include_globs=include,
                exclude_globs=exclude,
                predicate=predicate,
            )
            return

        if _looks_like_op(x, include_callables_with_dunder_call=True):
            _OP_REGISTRY.add(id(x))

    for obj in objs:
        _consume(obj)

_CAP_DEPTH = ContextVar("bs_cap_depth", default=0)

class _PatchSession:
    def __init__(self, frame: FrameType, gid: int) -> None:
        self.frame: FrameType = frame
        self.gid: int = gid

_WRAPPER_CACHE: Dict[int, Callable] = {}

_PATCH_LOCK = threading.RLock()
_REFCOUNT_BY_GID: Dict[int, int] = defaultdict(int)
_PATCHED_BY_GID: Dict[int, Dict[str, Any]] = {}

def _make_deferred(fn_or_cls: Union[Callable, Type]) -> Callable:
    """
    Wrapper: if capture is ON -> return CallObject(fn_or_cls)(*args, **kwargs),
    else call original.
    """
    cached = _WRAPPER_CACHE.get(id(fn_or_cls))
    if cached:
        return cached

    def _wrapper(*args, **kwargs):
        if _CAP_DEPTH.get() > 0:
            return CallObject(fn_or_cls)(*args, **kwargs)
        return fn_or_cls(*args, **kwargs)

    _wrapper.__name__ = getattr(fn_or_cls, "__name__", "_deferred")
    _wrapper.__qualname__ = getattr(fn_or_cls, "__qualname__", _wrapper.__name__)
    _wrapper.__doc__ = getattr(fn_or_cls, "__doc__", None)

    _WRAPPER_CACHE[id(fn_or_cls)] = _wrapper
    return _wrapper

def _pick_callsite_frame() -> FrameType:
    """
    Find the nearest caller frame whose globals contain at least one
    registered operation/class. This avoids patching pytest/stdlib frames.
    """
    cur = inspect.currentframe()
    assert cur is not None
    this_mod = __name__

    f = cur.f_back
    while f and f.f_globals.get("__name__") == this_mod:
        f = f.f_back

    probe = f
    while probe:
        g = probe.f_globals
        if any(id(obj) in _OP_REGISTRY for obj in g.values()):
            return probe
        probe = probe.f_back

    return f or cur

def begin_capture() -> _PatchSession:
    """
    Enable capture for the current thread/async task.
    Installs wrappers into the callsite module once globally (refcounted).
    """
    depth = _CAP_DEPTH.get()
    _CAP_DEPTH.set(depth + 1)

    frame = _pick_callsite_frame()
    gid = id(frame.f_globals)
    sess = _PatchSession(frame=frame, gid=gid)

    with _PATCH_LOCK:
        if _REFCOUNT_BY_GID[gid] == 0:
            originals: Dict[str, Any] = {}
            g = frame.f_globals
            for name, obj in list(g.items()):
                if is_registered(obj):
                    wrapper = _make_deferred(obj)
                    originals[name] = obj
                    g[name] = wrapper
            _PATCHED_BY_GID[gid] = originals
        _REFCOUNT_BY_GID[gid] += 1

    return sess

def end_capture(sess: _PatchSession) -> None:
    """
    Disable capture for the current thread/async task.
    Restores originals only when the last global user for this module ends.
    """
    depth = _CAP_DEPTH.get()
    _CAP_DEPTH.set(max(0, depth - 1))

    with _PATCH_LOCK:
        gid = sess.gid
        if gid in _REFCOUNT_BY_GID:
            _REFCOUNT_BY_GID[gid] -= 1
            if _REFCOUNT_BY_GID[gid] <= 0:
                g = sess.frame.f_globals
                originals = _PATCHED_BY_GID.pop(gid, {})
                for name, orig in originals.items():
                    current = g.get(name)
                    if current is _WRAPPER_CACHE.get(id(orig)):
                        g[name] = orig
                del _REFCOUNT_BY_GID[gid]


class _AttrProxy:
    """
    Non-live attribute path builder: tns.name.deep -> stores "tns.name.deep".
    Acts as a marker: hasattr(obj, "__bs_attrpath__") -> str.
    """
    __slots__ = ("__bs_attrpath__",)

    def __init__(self, path: str) -> None:
        object.__setattr__(self, "__bs_attrpath__", path)

    def __getattr__(self, name: str) -> "_AttrProxy":
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)
        return _AttrProxy(f"{self.__bs_attrpath__}.{name}")

    def __setattr__(self, key, value) -> None:
        raise TypeError("Attr proxy is read-only")

    def __call__(self, *args, **kwargs):
        raise TypeError(
            f"Attr proxy '{self.__bs_attrpath__}' is not callable; "
            f"use it only to build a path"
        )

    def __repr__(self) -> str:
        return f"<AttrProxy {self.__bs_attrpath__}>"

def alias_root(name: str) -> _AttrProxy:
    """
    Get a root proxy for an alias name (e.g. 'tns').
    IDE can chain attributes (tns.name); runtime sees a string via __bs_attrpath__.
    """
    return _AttrProxy(name)

_T = TypeVar("_T")

def typed_alias(name: str, typ: Type[_T]) -> _T:
    """
    Return an alias proxy typed as T (for IDE type hints and refactoring).
    At runtime this is an _AttrProxy carrying __bs_attrpath__.
    """
    return cast(_T, alias_root(name))
