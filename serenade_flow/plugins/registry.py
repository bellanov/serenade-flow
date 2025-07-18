import importlib

import logging


class PluginRegistry:
    """

    Minimal plugin registry for loading and managing pipeline plugins.

    """

    def __init__(self):

        self.plugins = {}

    def load_from_config(self, config):
        """

        Load plugins from a config dict (e.g., parsed from plugins.yaml).

        """

        for name, plugin_info in config.get("plugins", {}).items():

            if plugin_info.get("enabled", False):

                module_name = plugin_info["module"]

                class_name = plugin_info["class"]

                try:

                    module = importlib.import_module(module_name)

                    plugin_class = getattr(module, class_name)

                    self.plugins[name] = plugin_class()

                    logging.info(
                        f"Loaded plugin: {name} from {module_name}.{class_name}"
                    )

                except Exception as e:

                    logging.error(f"Failed to load plugin {name}: {e}")

    def get(self, name):

        return self.plugins.get(name)

    def all(self):

        return self.plugins
