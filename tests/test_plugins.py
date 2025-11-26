"""Tests for PluginRegistry."""

import pytest
import logging
from unittest.mock import patch, MagicMock
from serenade_flow.plugins import PluginRegistry


@pytest.fixture
def registry():
    """Create a PluginRegistry instance."""
    return PluginRegistry()


@pytest.mark.unit
def test_registry_initialization(registry):
    """Test that PluginRegistry initializes correctly."""
    assert registry.plugins == {}


@pytest.mark.unit
def test_load_from_config_success(registry):
    """Test loading plugins from config successfully."""
    # Mock a plugin module and class
    mock_plugin_class = MagicMock()
    mock_plugin_instance = MagicMock()
    mock_plugin_class.return_value = mock_plugin_instance

    mock_module = MagicMock()
    mock_module.TestPlugin = mock_plugin_class

    config = {
        "plugins": {
            "test_plugin": {
                "module": "test.module",
                "class": "TestPlugin",
                "enabled": True,
            }
        }
    }

    with patch("importlib.import_module", return_value=mock_module):
        registry.load_from_config(config)

    assert "test_plugin" in registry.plugins
    assert registry.plugins["test_plugin"] == mock_plugin_instance
    mock_plugin_class.assert_called_once()


@pytest.mark.unit
def test_load_from_config_disabled_plugin(registry):
    """Test that disabled plugins are not loaded."""
    config = {
        "plugins": {
            "test_plugin": {
                "module": "test.module",
                "class": "TestPlugin",
                "enabled": False,
            }
        }
    }

    registry.load_from_config(config)

    assert "test_plugin" not in registry.plugins


@pytest.mark.unit
def test_load_from_config_missing_enabled_key(registry):
    """Test that plugins without 'enabled' key are not loaded."""
    config = {
        "plugins": {
            "test_plugin": {
                "module": "test.module",
                "class": "TestPlugin",
            }
        }
    }

    registry.load_from_config(config)

    assert "test_plugin" not in registry.plugins


@pytest.mark.unit
def test_load_from_config_import_error(registry, caplog):
    """Test handling of import errors when loading plugins."""
    config = {
        "plugins": {
            "test_plugin": {
                "module": "nonexistent.module",
                "class": "TestPlugin",
                "enabled": True,
            }
        }
    }

    with caplog.at_level(logging.ERROR):
        registry.load_from_config(config)

    assert "test_plugin" not in registry.plugins
    assert any("Failed to load plugin" in msg for msg in caplog.text.splitlines())


@pytest.mark.unit
def test_load_from_config_attribute_error(registry, caplog):
    """Test handling of AttributeError when class doesn't exist."""
    mock_module = MagicMock()
    del mock_module.TestPlugin  # Make sure it doesn't exist

    config = {
        "plugins": {
            "test_plugin": {
                "module": "test.module",
                "class": "TestPlugin",
                "enabled": True,
            }
        }
    }

    with patch("importlib.import_module", return_value=mock_module):
        with caplog.at_level(logging.ERROR):
            registry.load_from_config(config)

    assert "test_plugin" not in registry.plugins
    assert any("Failed to load plugin" in msg for msg in caplog.text.splitlines())


@pytest.mark.unit
def test_get_existing_plugin(registry):
    """Test getting an existing plugin."""
    mock_plugin = MagicMock()
    registry.plugins["test_plugin"] = mock_plugin

    result = registry.get("test_plugin")

    assert result == mock_plugin


@pytest.mark.unit
def test_get_nonexistent_plugin(registry):
    """Test getting a nonexistent plugin."""
    result = registry.get("nonexistent")

    assert result is None


@pytest.mark.unit
def test_all_plugins(registry):
    """Test getting all plugins."""
    mock_plugin1 = MagicMock()
    mock_plugin2 = MagicMock()
    registry.plugins["plugin1"] = mock_plugin1
    registry.plugins["plugin2"] = mock_plugin2

    all_plugins = registry.all()

    assert len(all_plugins) == 2
    assert "plugin1" in all_plugins
    assert "plugin2" in all_plugins
    assert all_plugins["plugin1"] == mock_plugin1
    assert all_plugins["plugin2"] == mock_plugin2


@pytest.mark.unit
def test_load_from_config_multiple_plugins(registry):
    """Test loading multiple plugins from config."""
    mock_plugin_class1 = MagicMock()
    mock_plugin_instance1 = MagicMock()
    mock_plugin_class1.return_value = mock_plugin_instance1

    mock_plugin_class2 = MagicMock()
    mock_plugin_instance2 = MagicMock()
    mock_plugin_class2.return_value = mock_plugin_instance2

    mock_module1 = MagicMock()
    mock_module1.Plugin1 = mock_plugin_class1

    mock_module2 = MagicMock()
    mock_module2.Plugin2 = mock_plugin_class2

    config = {
        "plugins": {
            "plugin1": {
                "module": "test.module1",
                "class": "Plugin1",
                "enabled": True,
            },
            "plugin2": {
                "module": "test.module2",
                "class": "Plugin2",
                "enabled": True,
            },
        }
    }

    def import_module_side_effect(module_name):
        if module_name == "test.module1":
            return mock_module1
        elif module_name == "test.module2":
            return mock_module2
        return None

    with patch("importlib.import_module", side_effect=import_module_side_effect):
        registry.load_from_config(config)

    assert "plugin1" in registry.plugins
    assert "plugin2" in registry.plugins
    assert registry.plugins["plugin1"] == mock_plugin_instance1
    assert registry.plugins["plugin2"] == mock_plugin_instance2


@pytest.mark.unit
def test_load_from_config_no_plugins_key(registry):
    """Test loading from config with no plugins key."""
    config = {}

    registry.load_from_config(config)

    assert len(registry.plugins) == 0


@pytest.mark.unit
def test_load_from_config_empty_plugins(registry):
    """Test loading from config with empty plugins dict."""
    config = {"plugins": {}}

    registry.load_from_config(config)

    assert len(registry.plugins) == 0
