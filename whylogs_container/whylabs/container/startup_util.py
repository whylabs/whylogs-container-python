# pyright: reportAttributeAccessIssue=false,reportUnknownLambdaType=false
import types

_to_mock = [
    "torch",
    "sentence_transformers",
    "onnxruntime",
    "transformers",
    "whylabs_llm_toolkit",
    "langkit.core.workflow",
    "langkit.core.validation",
    "langkit.validators.comparison",
    "langkit.validators.library",
    "whylabs_llm_toolkit.rulesets.ruleset",
    "langkit.callbacks.library",
    "langkit.core.metric",
    "langkit.core.validation",
    "langkit.core.workflow",
    "langkit.metrics.library",
    "langkit.transformer",
]


class AutoMock(types.ModuleType):
    """
    Custom ModuleType that automatically mocks attribute accesses.
    """

    def __getattr__(self, name):  # pyright: ignore[reportUnknownParameterType]
        # Return a simple lambda function that returns None for any attribute
        # This can be customized to return more complex mocks if needed
        if name in ["__path__", "__all__", "keys", "values", "items"]:
            return []  # pyright: ignore[reportUnknownVariableType]
        return lambda *args, **kwargs: None  # pyright: ignore[reportUnknownVariableType]


def fake_llm_deps():
    """
    Hack to make sure that the non-llm container doesn't die because torch is missing
    """
    for module in _to_mock:
        _mock_module_if_missing(module)


def _mock_module_if_missing(import_path: str):
    import sys

    """
    Attempt to import a module by its path. If the module doesn't exist,
    create a mock module and register it in sys.modules.

    Args:
        import_path (str): The import path of the module to mock.
    """
    # try:
    #     __import__(import_path)
    # except ImportError:
    parts = import_path.split(".")
    module_name = parts[-1]
    parent_path = ".".join(parts[:-1])

    # Use AutoMock instead of types.ModuleType
    module = AutoMock(module_name)

    if parent_path:
        parent_module = _mock_module_if_missing(parent_path)
        setattr(parent_module, module_name, module)
        sys.modules[parent_path + "." + module_name] = module
    else:
        sys.modules[import_path] = module

    return sys.modules[import_path]
