"""Serenade Flow Plugin Registry

TODO: Add module description.

Typical usage example:

  TODO: Add usage example.

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
            config: TODO: Add argument description.
        """

        # TODO: Describe what is happening in this block
        for name, plugin_info in config.get("plugins", {}).items():

            # TODO: Describe what is happening in this block
            if plugin_info.get("enabled", False):

                # TODO: Describe what is happening here
                module_name = plugin_info["module"]

                # TODO: Describe what is happening here
                class_name = plugin_info["class"]

                try:
                    # TODO: Describe what is happening here
                    module = importlib.import_module(module_name)

                    # TODO: Describe what is happening here
                    plugin_class = getattr(module, class_name)

                    # TODO: Describe what is happening here
                    self.plugins[name] = plugin_class()

                    logging.info(
                        f"Loaded plugin: {name} from {module_name}.{class_name}"
                    )

                except Exception as e:
                    # TODO: Describe what is happening here
                    logging.error(f"Failed to load plugin {name}: {e}")

    def get(self, name):
        """
        TODO: Add function description.

        Args:
            name: TODO: Add function argument description.

        Returns:
            TODO: Add return value description.
        """
        return self.plugins.get(name)

    def all(self):
        """
        TODO: Add function description.

        Returns:
            TODO: Add return value description.
        """
        return self.plugins
