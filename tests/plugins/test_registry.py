"""Tests for PluginRegistry."""

from unittest.mock import MagicMock, patch

import pytest

from serenade_flow.plugins.registry import PluginRegistry


@pytest.fixture  # type: ignore
def registry() -> PluginRegistry:
    """Create a PluginRegistry instance."""
    return PluginRegistry()


@pytest.mark.unit  # type: ignore
def test_init(registry: PluginRegistry):
    """Test PluginRegistry initialization.

    Args:
        registry: PluginRegistry instance for testing.
    """
    assert registry.plugins == {}


@pytest.mark.unit  # type: ignore
def test_load_from_config_enabled_plugin(registry: PluginRegistry):
    """Test loading an enabled plugin from config.

    Args:
        registry: PluginRegistry instance for testing.
    """
    mock_plugin_class = MagicMock()
    mock_plugin_instance = MagicMock()
    mock_plugin_class.return_value = mock_plugin_instance

    with patch("importlib.import_module") as mock_import:
        mock_module = MagicMock()
        mock_module.TestPlugin = mock_plugin_class
        mock_import.return_value = mock_module

        config = {
            "plugins": {
                "test_plugin": {
                    "module": "test_module",
                    "class": "TestPlugin",
                    "enabled": True,
                }
            }
        }

        registry.load_from_config(config)

        assert "test_plugin" in registry.plugins
        mock_import.assert_called_once_with("test_module")
        mock_plugin_class.assert_called_once()


@pytest.mark.unit  # type: ignore
def test_load_from_config_disabled_plugin(registry: PluginRegistry):
    """Test that disabled plugins are not loaded.

    Args:
        registry: PluginRegistry instance for testing.
    """
    config = {
        "plugins": {
            "test_plugin": {
                "module": "test_module",
                "class": "TestPlugin",
                "enabled": False,
            }
        }
    }

    registry.load_from_config(config)

    assert "test_plugin" not in registry.plugins
    assert registry.plugins == {}


@pytest.mark.unit  # type: ignore
def test_load_from_config_no_plugins_key(registry: PluginRegistry):
    """Test loading from config with no plugins key.

    Args:
        registry: PluginRegistry instance for testing.
    """
    config = {}

    registry.load_from_config(config)

    assert registry.plugins == {}


@pytest.mark.unit  # type: ignore
def test_load_from_config_empty_plugins(registry: PluginRegistry):
    """Test loading from config with empty plugins dict.

    Args:
        registry: PluginRegistry instance for testing.
    """
    config = {"plugins": {}}

    registry.load_from_config(config)

    assert registry.plugins == {}


@pytest.mark.unit  # type: ignore
def test_load_from_config_import_error(registry: PluginRegistry, caplog):
    """Test handling of import errors when loading plugins.

    Args:
        registry: PluginRegistry instance for testing.
        caplog: Pytest fixture for capturing log messages.
    """
    with patch("importlib.import_module") as mock_import:
        mock_import.side_effect = ImportError("Module not found")

        config = {
            "plugins": {
                "test_plugin": {
                    "module": "nonexistent_module",
                    "class": "TestPlugin",
                    "enabled": True,
                }
            }
        }

        registry.load_from_config(config)

        assert "test_plugin" not in registry.plugins
        assert "Failed to load plugin test_plugin" in caplog.text


@pytest.mark.unit  # type: ignore
def test_load_from_config_missing_class(registry: PluginRegistry, caplog):
    """Test handling of missing class attribute in module.

    Args:
        registry: PluginRegistry instance for testing.
        caplog: Pytest fixture for capturing log messages.
    """
    with patch("importlib.import_module") as mock_import:
        mock_module = MagicMock()
        del mock_module.NonexistentPlugin
        mock_import.return_value = mock_module

        config = {
            "plugins": {
                "test_plugin": {
                    "module": "test_module",
                    "class": "NonexistentPlugin",
                    "enabled": True,
                }
            }
        }

        registry.load_from_config(config)

        assert "test_plugin" not in registry.plugins
        assert "Failed to load plugin test_plugin" in caplog.text


@pytest.mark.unit  # type: ignore
def test_load_from_config_instantiation_error(registry: PluginRegistry, caplog):
    """Test handling of errors during plugin instantiation.

    Args:
        registry: PluginRegistry instance for testing.
        caplog: Pytest fixture for capturing log messages.
    """
    mock_plugin_class = MagicMock()
    mock_plugin_class.side_effect = Exception("Instantiation failed")

    with patch("importlib.import_module") as mock_import:
        mock_module = MagicMock()
        mock_module.TestPlugin = mock_plugin_class
        mock_import.return_value = mock_module

        config = {
            "plugins": {
                "test_plugin": {
                    "module": "test_module",
                    "class": "TestPlugin",
                    "enabled": True,
                }
            }
        }

        registry.load_from_config(config)

        assert "test_plugin" not in registry.plugins
        assert "Failed to load plugin test_plugin" in caplog.text


@pytest.mark.unit  # type: ignore
def test_load_from_config_multiple_plugins(registry: PluginRegistry):
    """Test loading multiple plugins from config.

    Args:
        registry: PluginRegistry instance for testing.
    """
    mock_plugin1_class = MagicMock()
    mock_plugin1_instance = MagicMock()
    mock_plugin1_class.return_value = mock_plugin1_instance

    mock_plugin2_class = MagicMock()
    mock_plugin2_instance = MagicMock()
    mock_plugin2_class.return_value = mock_plugin2_instance

    with patch("importlib.import_module") as mock_import:
        mock_module = MagicMock()
        mock_module.Plugin1 = mock_plugin1_class
        mock_module.Plugin2 = mock_plugin2_class
        mock_import.return_value = mock_module

        config = {
            "plugins": {
                "plugin1": {
                    "module": "test_module",
                    "class": "Plugin1",
                    "enabled": True,
                },
                "plugin2": {
                    "module": "test_module",
                    "class": "Plugin2",
                    "enabled": True,
                },
            }
        }

        registry.load_from_config(config)

        assert "plugin1" in registry.plugins
        assert "plugin2" in registry.plugins
        assert len(registry.plugins) == 2


@pytest.mark.unit  # type: ignore
def test_get_existing_plugin(registry: PluginRegistry):
    """Test retrieving an existing plugin.

    Args:
        registry: PluginRegistry instance for testing.
    """
    mock_plugin = MagicMock()
    registry.plugins["test_plugin"] = mock_plugin

    result = registry.get("test_plugin")

    assert result == mock_plugin


@pytest.mark.unit  # type: ignore
def test_get_nonexistent_plugin(registry: PluginRegistry):
    """Test retrieving a non-existent plugin.

    Args:
        registry: PluginRegistry instance for testing.
    """
    result = registry.get("nonexistent_plugin")

    assert result is None


@pytest.mark.unit  # type: ignore
def test_all(registry: PluginRegistry):
    """Test retrieving all plugins.

    Args:
        registry: PluginRegistry instance for testing.
    """
    mock_plugin1 = MagicMock()
    mock_plugin2 = MagicMock()
    registry.plugins["plugin1"] = mock_plugin1
    registry.plugins["plugin2"] = mock_plugin2

    result = registry.all()

    assert result == registry.plugins
    assert "plugin1" in result
    assert "plugin2" in result
    assert len(result) == 2
