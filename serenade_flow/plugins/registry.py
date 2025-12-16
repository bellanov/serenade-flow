"""Serenade Flow Plugin Registry

This module provides a plugin registry for dynamically loading and managing pipeline plugins.

Typical usage example:

  from serenade_flow.plugins import PluginRegistry

  registry = PluginRegistry()
  config = {
      "plugins": {
          "my_plugin": {
              "module": "my_module",
              "class": "MyPlugin",
              "enabled": True
          }
      }
  }
  registry.load_from_config(config)
  plugin = registry.get("my_plugin")

"""

import importlib
import logging


class PluginRegistry:
    """
    Minimal plugin registry for loading and managing pipeline plugins.
    """

    def __init__(self):
        """Initialize Class Variables."""
        self.plugins = {}

    def load_from_config(self, config):
        """
        Load plugins from a config dict (e.g., parsed from plugins.yaml).

        Args:
            config: Configuration dictionary containing plugin definitions.
        """
        # Iterate through all plugins defined in the configuration
        for name, plugin_info in config.get("plugins", {}).items():

            # Only load plugins that are explicitly enabled
            if plugin_info.get("enabled", False):

                # Extract the module name where the plugin class is defined
                module_name = plugin_info["module"]

                # Extract the class name of the plugin to instantiate
                class_name = plugin_info["class"]

                try:
                    # Dynamically import the module containing the plugin class
                    module = importlib.import_module(module_name)

                    # Get the plugin class from the imported module
                    plugin_class = getattr(module, class_name)

                    # Instantiate the plugin and store it in the registry
                    self.plugins[name] = plugin_class()

                    logging.info(
                        f"Loaded plugin: {name} from {module_name}.{class_name}"
                    )

                except Exception as e:
                    # Log error if plugin loading fails but continue with other plugins
                    logging.error(f"Failed to load plugin {name}: {e}")

    def get(self, name):
        """
        Retrieve a plugin by name.

        Args:
            name: Name of the plugin to retrieve.

        Returns:
            The plugin instance if found, None otherwise.
        """
        return self.plugins.get(name)

    def all(self):
        """
        Get all registered plugins.

        Returns:
            Dictionary mapping plugin names to their instances.
        """
        return self.plugins
