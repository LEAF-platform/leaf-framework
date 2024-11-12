import json
import logging
import os
import re
from typing import Any, Optional

import yaml
from core.modules.logger_modules.logger_utils import get_logger

logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)

equipment_key = "equipment"


class MetadataManager:
    def __init__(self) -> None:
        """Initialize the metadata dictionary for each adapter."""
        logger.info("Initializing MetadataManager")
        self._metadata: dict = {}
        self.equipment_terms: Optional[EquipmentTerms] = None
        self.required_keys: set[str] = set()
        
        # Load both equipment terms and required fields
        self.load_equipment_terms()
        self.load_required_keys()

    def load_equipment_terms(self):
        """Load YAML configuration into equipment terms."""
        curr_dir = os.path.dirname(os.path.realpath(__file__))
        filepath = os.path.join(curr_dir, "equipment_actions.yaml")
        try:
            with open(filepath, "r") as file:
                yaml_content = yaml.safe_load(file)
                self.equipment_terms = EquipmentTerms(yaml_content, self._metadata)
        except FileNotFoundError:
            print(f"YAML file {filepath} not found.")

    def load_required_keys(self):
        """Load required keys from the second YAML document."""
        curr_dir = os.path.dirname(os.path.realpath(__file__))
        filepath = os.path.join(curr_dir, "equipment_data.yaml")
        try:
            with open(filepath, "r") as file:
                yaml_content = yaml.safe_load(file)
                self.required_keys = set(yaml_content.keys())
        except FileNotFoundError:
            print(f"Required fields YAML file {filepath} not found.")

    def load_from_file(self, file_path, adapter_type=None) -> None:
        """Load metadata from a JSON file and update the metadata dictionary."""
        logger.debug(f"Loading metadata from file {file_path}")
        try:
            with open(file_path, "r") as file:
                if adapter_type is not None:
                    self._metadata.setdefault(adapter_type, {}).update(json.load(file))
                else:
                    self._metadata.update(json.load(file))
        except FileNotFoundError:
            print(f"Metadata file {file_path} not found.")

    def get_metadata(self, key: str, default: Any=None) -> Any:
        """Retrieve a specific metadata value."""
        return self._metadata.get(key, default)

    def add_metadata(self, key: str, value: str) -> None:
        """Set a specific metadata value."""
        self._metadata[key] = value

    def get_equipment_data(self) -> dict[str, str]:
        return self._metadata.get(equipment_key, {})

    def add_equipment_data(self, filename: str) -> None:
        if isinstance(filename, dict):
            self._metadata.setdefault(equipment_key, {}).update(filename)
        else:
            self.load_from_file(filename, equipment_key)

    def is_called(self, action: str, term: str) -> bool:
        return action.split("/")[-1] == term.split("/")[-1]

    def get_instance_id(self, topic: str=None) -> str:
        if topic:
            return topic.split("/")[2]
        return self._metadata.get(equipment_key, {}).get("instance_id", "")

    def is_valid(self) -> bool:
        """Check if all required keys are present in the metadata."""
        missing_keys = [key for key in self.required_keys if key not in self._metadata.get(equipment_key, {})]
        if missing_keys:
            logger.warning(f"Missing required keys in metadata: {missing_keys}")
        return not missing_keys

    def __getattr__(self, item: str) -> Any:
        """Dynamically handle attribute access based on equipment terms."""
        if hasattr(self.equipment_terms, item):
            return getattr(self.equipment_terms, item)
        raise AttributeError(f"'MetadataManager' object has no attribute '{item}'")


class EquipmentTerms:
    def __init__(self, dictionary: dict[str, Any], metadata: dict[str, Any]) -> None:
        """
        Initialize EquipmentTerms with YAML dictionary and metadata.
        The metadata dictionary is used to dynamically replace
        placeholders like <institute>.
        """
        self._metadata = metadata
        for key, value in dictionary.items():
            if isinstance(value, dict):
                setattr(self, key, EquipmentTerms(value, metadata))
            else:
                setattr(self, key, self._create_function(value))

    def _create_function(self, path_template: str) -> Any:
        """
        Create a function that replaces placeholders with metadata values.
        The placeholders like <institute> are dynamically replaced with the values
        from the metadata dictionary, but can be overridden by arguments.
        """

        def replace_placeholders(**kwargs) -> str:
            return re.sub(
                r"<([^>]+)>",
                lambda match: kwargs.get(
                    match.group(1), self._get_metadata_value(match.group(1))
                ),
                path_template,
            )

        return replace_placeholders

    def _get_metadata_value(self, key: str) -> str:
        """
        Get the metadata value for a given key.
        The key should correspond directly to the
        structure in metadata, e.g., <institute>
        should map to self._metadata["institute"].
        """
        try:
            return str(self._metadata[equipment_key][key])
        except KeyError:
            return f"+"

    def __repr__(self) -> str:
        return f"{self.__dict__}"
